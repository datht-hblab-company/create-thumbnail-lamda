// index.mjs
import { S3Client, GetObjectCommand, PutObjectCommand, HeadObjectCommand } from "@aws-sdk/client-s3";
import sharp from "sharp";
import path from "node:path";

const s3 = new S3Client({});

// Kích thước cần tạo
const WIDTHS = [50, 100, 200, 400, 600, 800, 1200];

const {
  DEST_PREFIX = "resized/",
  JPEG_QUALITY = "85",
  WEBP_QUALITY = "85",
  PNG_COMPRESSION = "9",
  ALLOWED_SECTIONS = "",
  // ép kích thước raster tối thiểu cho SVG trước khi convert (để nét)
  SVG_BASE_WIDTH_MIN = "1200",
  // "lossless" | "lossy" (dùng WEBP_QUALITY); mặc định giữ "lossy"
  WEBP_SVG_MODE = "lossy"
} = process.env;

const ALLOW = new Set(ALLOWED_SECTIONS.split(",").map(s => s.trim()).filter(Boolean));

// Chỉ xử lý các ext này (có SVG)
const ALLOWED_EXTS = new Set(["png", "jpg", "jpeg", "jfif", "svg"]);

const CONTENT_TYPE_BY_EXT = {
  jpg:  "image/jpeg",
  jpeg: "image/jpeg",
  jfif: "image/jpeg",
  png:  "image/png",
  webp: "image/webp",
  svg:  "image/svg+xml"
};

export const handler = async (event) => {
  const results = [];
  for (const record of event.Records ?? []) {
    try {
      const body = JSON.parse(record.body);
      const s3Records = body.Records ?? [];
      for (const r of s3Records) {
        if (!r.s3) continue;

        const bucket = r.s3.bucket.name;
        const key = decodeURIComponent(r.s3.object.key.replace(/\+/g, " "));

        const { firstSegment, remainder } = splitFirstSegment(key);
        if (!firstSegment || !remainder) continue;

        const destRoot = DEST_PREFIX.replace(/\/$/, "");
        if (firstSegment === destRoot) {
          console.log("Skip already-resized:", key);
          continue;
        }
        if (ALLOW.size > 0 && !ALLOW.has(firstSegment)) {
          console.log(`Skip by whitelist: ${firstSegment} not in`, [...ALLOW]);
          continue;
        }

        const base = path.posix.basename(remainder);
        const extFromName = (base.split(".").pop() || "").toLowerCase();
        if (!ALLOWED_EXTS.has(extFromName)) {
          console.log("Skip unsupported ext:", key);
          continue;
        }

        const res = await processOneObject({
          bucket,
          srcKey: key,
          firstSegment,
          remainder,
          destRoot,
          extFromName
        });
        results.push(res);
      }
    } catch (err) {
      console.error("Error handling SQS record:", err);
      throw err;
    }
  }
  return { statusCode: 200, body: JSON.stringify({ ok: true, results }) };
};

// ---------- Core ----------

function splitFirstSegment(key) {
  const norm = key.replace(/^\/+/, "");
  const parts = norm.split("/");
  const firstSegment = parts[0] || "";
  const remainder = parts.slice(1).join("/");
  return { firstSegment, remainder };
}

async function processOneObject({ bucket, srcKey, firstSegment, remainder, destRoot, extFromName }) {
  const dir = path.posix.dirname(remainder);
  const base = path.posix.basename(remainder);
  const name = base.slice(0, -(extFromName.length + 1));

  const originalObj = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: srcKey }));
  const originalBuffer = await streamToBuffer(originalObj.Body);

  // Tạo sharp instance (density hữu ích khi SVG thiếu size)
  const img = sharp(originalBuffer, { failOn: "none", density: 300 });
  const meta = await img.metadata();
  const maxOriginalW = meta.width || Infinity;

  const normalizedInputExt = normalizeExt(extFromName, meta.format);
  if (!ALLOWED_EXTS.has(normalizedInputExt)) {
    console.log("Skip after normalize:", srcKey, "->", normalizedInputExt);
    return { bucket, key: srcKey, ok: false, reason: "unsupported_ext" };
  }

  for (const w of WIDTHS) {
    const targetW = Math.min(w, maxOriginalW);

    // ----- SVG branch (copy .svg + chỉnh size trong SVG trước khi convert WebP) -----
    if (normalizedInputExt === "svg") {
      // 4.1) Copy nguyên SVG ra -wXX.svg (vector)
      const dstKeySvgCopy = path.posix.join(
        destRoot, firstSegment, dir === "." ? "" : dir, `${name}-w${w}.svg`
      ).replace(/\/+/g, "/");

      await s3.send(new PutObjectCommand({
        Bucket: bucket,
        Key: dstKeySvgCopy,
        Body: originalBuffer,
        ContentType: CONTENT_TYPE_BY_EXT.svg
      }));

      // 4.2) Convert WebP: sửa width/height ngay trong SVG rồi mới raster
      const baseWidthMin = parseInt(SVG_BASE_WIDTH_MIN, 10) || 1200;
      const rasterW = Math.max(baseWidthMin, targetW); // ép tối thiểu 1200 như bạn muốn

      const prepared = prepareSvgForRaster(originalBuffer, rasterW);
      const webpOpts = (WEBP_SVG_MODE === "lossless")
        ? { lossless: true }
        : { quality: parseInt(WEBP_QUALITY, 10) };

      const webpBuf = await sharp(prepared)
        // tiếp tục resize về đúng targetW để tạo đúng biến thể (nếu rasterW > targetW)
        .resize({ width: targetW })
        .webp(webpOpts)
        .toBuffer();

      const dstKeyWebp = path.posix.join(
        destRoot, firstSegment, dir === "." ? "" : dir, `${name}-w${w}.webp`
      ).replace(/\/+/g, "/");

      await s3.send(new PutObjectCommand({
        Bucket: bucket,
        Key: dstKeyWebp,
        Body: webpBuf,
        ContentType: "image/webp"
      }));

      continue;
    }

    // ----- Các định dạng còn lại: giữ nguyên logic cũ -----
    const { buffer: resizedBuf, outExt, contentType } = await toOriginalFormat(
      img, targetW, normalizedInputExt, {
        jpegQuality: parseInt(JPEG_QUALITY, 10),
        pngCompression: parseInt(PNG_COMPRESSION, 10)
      }
    );

    const dstKeyOriginal = path.posix.join(
      destRoot, firstSegment, dir === "." ? "" : dir, `${name}-w${w}.${outExt}`
    ).replace(/\/+/g, "/");

    await s3.send(new PutObjectCommand({
      Bucket: bucket,
      Key: dstKeyOriginal,
      Body: resizedBuf,
      ContentType: contentType
    }));

    const webpBuf = await img.clone()
      .resize({ width: targetW, withoutEnlargement: true })
      .webp({ quality: parseInt(WEBP_QUALITY, 10) })
      .toBuffer();

    const dstKeyWebp = path.posix.join(
      destRoot, firstSegment, dir === "." ? "" : dir, `${name}-w${w}.webp`
    ).replace(/\/+/g, "/");

    await s3.send(new PutObjectCommand({
      Bucket: bucket,
      Key: dstKeyWebp,
      Body: webpBuf,
      ContentType: "image/webp"
    }));
  }

  return { bucket, key: srcKey, ok: true };
}

/**
 * GIỮ NGUYÊN ext gốc nếu thuộc ALLOWED_EXTS.
 */
function normalizeExt(ext, metaFormat) {
  const e = (ext || "").toLowerCase();
  const f = (metaFormat || "").toLowerCase();
  if (ALLOWED_EXTS.has(e)) return e;
  if (f === "jpeg") return "jpeg";
  if (f === "png")  return "png";
  if (f === "svg")  return "svg";
  return "jpg";
}

/**
 * PNG/JPG/JPEG/JFIF → xuất đúng định dạng gốc.
 */
async function toOriginalFormat(img, width, ext, opts) {
  const base = img.clone().resize({ width, withoutEnlargement: true });
  switch (ext) {
    case "jpg":
    case "jpeg":
    case "jfif":
      return {
        buffer: await base.jpeg({ quality: opts.jpegQuality, mozjpeg: true }).toBuffer(),
        outExt: ext,
        contentType: CONTENT_TYPE_BY_EXT[ext]
      };
    case "png":
      return {
        buffer: await base.png({ compressionLevel: opts.pngCompression }).toBuffer(),
        outExt: "png",
        contentType: CONTENT_TYPE_BY_EXT.png
      };
    default:
      return {
        buffer: await base.jpeg({ quality: opts.jpegQuality, mozjpeg: true }).toBuffer(),
        outExt: "jpg",
        contentType: CONTENT_TYPE_BY_EXT.jpg
      };
  }
}

/**
 * Sửa width/height ngay trong nội dung SVG theo targetW.
 * - Ưu tiên lấy tỉ lệ từ viewBox (w,h); fallback lấy từ width/height hiện có; cuối cùng fallback 1:1.
 * - Trả về Buffer SVG đã chỉnh.
 */
function prepareSvgForRaster(svgBuffer, targetW) {
  let svg = svgBuffer.toString("utf8");

  // Lấy tỉ lệ từ viewBox nếu có
  const vb = svg.match(/viewBox=["']\s*([0-9.+-]+)\s+([0-9.+-]+)\s+([0-9.+-]+)\s+([0-9.+-]+)\s*["']/i);
  let ratio = null;
  if (vb) {
    const vbw = parseFloat(vb[3]);
    const vbh = parseFloat(vb[4]);
    if (vbw > 0 && vbh > 0) ratio = vbh / vbw;
  }
  // Fallback: lấy từ width/height hiện có
  if (!ratio) {
    const wAttr = svg.match(/width=["']\s*([0-9.]+)(?:px)?\s*["']/i);
    const hAttr = svg.match(/height=["']\s*([0-9.]+)(?:px)?\s*["']/i);
    if (wAttr && hAttr) {
      const w = parseFloat(wAttr[1]);
      const h = parseFloat(hAttr[1]);
      if (w > 0 && h > 0) ratio = h / w;
    }
  }
  if (!ratio) ratio = 1; // fallback 1:1 nếu bất khả kháng

  const targetH = Math.round(targetW * ratio);

  // Cập nhật/chen width/height vào thẻ <svg ...>
  svg = svg.replace(/<svg\b([^>]*)>/i, (m, attrs) => {
    let a = attrs;
    const hasW = /width=["'][^"']*["']/i.test(a);
    const hasH = /height=["'][^"']*["']/i.test(a);
    if (hasW) a = a.replace(/width=["'][^"']*["']/i, `width="${targetW}"`);
    if (hasH) a = a.replace(/height=["'][^"']*["']/i, `height="${targetH}"`);
    if (!hasW) a += ` width="${targetW}"`;
    if (!hasH) a += ` height="${targetH}"`;
    return `<svg${a}>`;
  });

  return Buffer.from(svg, "utf8");
}

async function streamToBuffer(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(chunk);
  return Buffer.concat(chunks);
}

async function exists(bucket, key) {
  try {
    await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
    return true;
  } catch {
    return false;
  }
}
