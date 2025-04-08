import { S3 } from '@aws-sdk/client-s3';
import ffmpeg from 'fluent-ffmpeg';
import fs from 'fs';
import path from 'path';
import axios from 'axios';
import { createHmac } from 'crypto';

const s3 = new S3();

async function callWebhookWithRetry(payload, apiKey, signature, maxRetries = 3, initialDelay = 2000) {
    for (let attempt = 0; attempt < maxRetries; attempt++) {
        try {
            const webhookResponse = await axios.post(
                process.env.WEBHOOK_URL,
                payload,
                {
                    headers: {
                        'Content-Type': 'application/json',
                        'x-api-key': apiKey,
                        'x-signature': signature
                    }
                }
            );
            console.log('Webhook response:', webhookResponse.data);
            return webhookResponse;
        } catch (error) {
            console.error(`Webhook attempt ${attempt + 1} failed:`, error.message);
            
            if (attempt === maxRetries - 1) {
                throw error; // Throw on final attempt
            }
            
            // Calculate delay with exponential backoff
            const delay = initialDelay * Math.pow(2, attempt);
            console.log(`Waiting ${delay}ms before retry...`);
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }
}

export const handler = async (event) => {
    try {
        // Get bucket and file information from SQS event
        console.log("Event received:", JSON.stringify(event, null, 2));

        // Validate SQS event
        if (!event.Records || event.Records.length === 0) {
            console.log("No Records found in event");
            return { statusCode: 400, body: "No Records found in event" };
        }
    
        // Parse SQS message to get S3 event
        const record = event.Records[0];
        const s3Event = JSON.parse(record.body);
        const s3Record = s3Event.Records[0];
    
        if (!s3Record.s3 || !s3Record.s3.bucket || !s3Record.s3.bucket.name || !s3Record.s3.object || !s3Record.s3.object.key) {
            console.log("Incomplete S3 data:", JSON.stringify(s3Record.s3, null, 2));
            return { statusCode: 400, body: "Incomplete S3 data" };
        }
    
        const bucket = s3Record.s3.bucket.name;
        const key = decodeURIComponent(s3Record.s3.object.key.replace(/\+/g, ' '));
    
        console.log(`Bucket: ${bucket}, Key: ${key}`);
        
        // Check if the file is a video
        if (!key.match(/\.(mp4|mov|avi|wmv|flv|mkv)$/i)) {
            console.log('File is not a video, skipping.');
            return { statusCode: 200, body: 'File is not a video' };
        }
        
        // Temporary file paths
        const videoPath = `/tmp/${path.basename(key)}`;
        const thumbnailPath = `/tmp/${path.basename(key).split('.').slice(0, -1).join('.')}.jpg`;
        
        // Download video from S3
        const videoObject = await s3.getObject({ Bucket: bucket, Key: key });
        const videoBuffer = await streamToBuffer(videoObject.Body);
        fs.writeFileSync(videoPath, videoBuffer);
        
        // Create thumbnail from the first frame of the video
        await new Promise((resolve, reject) => {
            console.log(`Starting thumbnail creation from: ${videoPath}`);
            ffmpeg(videoPath)
                .on('start', (cmd) => console.log("FFmpeg command:", cmd))
                .on('progress', (progress) => console.log("Processing:", progress))
                .on('end', () => {
                    console.log("Thumbnail created:", thumbnailPath);
                    resolve();
                })
                .on('error', (err) => {
                    console.error("FFmpeg error:", err);
                    reject(err);
                })
                .screenshots({
                    count: 1,
                    filename: path.basename(thumbnailPath),
                    folder: '/tmp',
                    timemarks: ['00:00:01']
                });
        });
        
        // Upload thumbnail to S3
        const thumbnailKey = `thumbnails/${path.basename(thumbnailPath)}`;
        await s3.putObject({
            Bucket: bucket,
            Key: thumbnailKey,
            Body: fs.readFileSync(thumbnailPath),
            ContentType: 'image/jpeg'
        });
        
        // Clean up temporary files
        fs.unlinkSync(videoPath);
        fs.unlinkSync(thumbnailPath);

        // Prepare webhook payload
        const payload = {
            videoPath: key,
            thumbnailPath: thumbnailKey
        };

        const apiKey = process.env.WEBHOOK_API_KEY;
        const secretKey = process.env.WEBHOOK_SECRET_KEY;

        const hmac = createHmac('sha256', secretKey);
        hmac.update(JSON.stringify(payload));
        const signature = hmac.digest('hex');

        // Call webhook API
        try {
            await callWebhookWithRetry(payload, apiKey, signature);
        } catch (webhookError) {
            console.error('All webhook retries failed:', webhookError);
            throw webhookError; // Re-throw the error to let SQS handle retry
        }

        return {
            statusCode: 200,
            body: JSON.stringify({
                message: 'Thumbnail created and webhook notified successfully',
                thumbnailKey: thumbnailKey
            })
        };
    } catch (error) {
        console.error('Error:', error);
        // With SQS trigger, we should throw the error to let SQS handle retry
        throw error;
    }
};

// Helper function to convert stream to buffer
async function streamToBuffer(stream) {
    return new Promise((resolve, reject) => {
        const chunks = [];
        stream.on('data', (chunk) => chunks.push(chunk));
        stream.on('error', reject);
        stream.on('end', () => resolve(Buffer.concat(chunks)));
    });
}
