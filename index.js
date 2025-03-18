// ✅ Shopify and OpenAI Alt Text Generator with Debug Logging, Pagination, Caching, Batching, and Rate Handling

require("dotenv").config();
const fs = require("fs");
const csv = require("csv-parser");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
const axios = require("axios");

const SHOPIFY_API_KEY = process.env.SHOPIFY_API_KEY;
const SHOPIFY_SHOP_DOMAIN = process.env.SHOPIFY_SHOP_DOMAIN;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

const inputCsv = "images.csv";
const outputCsv = "updated_images_with_alt_text.csv";

const batchSize = 500; // Adjust batch size as needed
const openAiChunkSize = 30; // Process 5 images at a time to avoid hitting rate limits
const openAiChunkDelay = 20000; // 20 seconds delay between chunks to control API usage
const openAiRateLimit = 30000; // Tokens per minute limit
const openAiRequestDelay = Math.ceil((60 / (openAiRateLimit / 880)) * 1000); // Calculate delay per request
let shopifyFiles = {};
const cacheFile = "shopify_files_cache.json";

console.log("🚀 Starting script execution...");

// ✅ Safe Shopify GraphQL Query with Retry
async function safeShopifyQuery(query) {
  console.log("📡 Sending Shopify GraphQL request...");
  while (true) {
    try {
      const response = await axios.post(
        `https://${SHOPIFY_SHOP_DOMAIN}/admin/api/2023-01/graphql.json`,
        { query }, // ✅ Ensure the query is wrapped inside an object
        {
          headers: {
            "X-Shopify-Access-Token": SHOPIFY_API_KEY,
            "Content-Type": "application/json",
            Accept: "application/json", // ✅ Ensure response is JSON
          },
        }
      );
      console.log("✅ Shopify GraphQL request successful.");
      return response;
    } catch (error) {
      console.error("❌ Shopify GraphQL request failed: ", error.message);
      if (error.response?.status === 429) {
        console.log("⚠️ Rate limited, retrying in 5 seconds...");
        await new Promise((resolve) => setTimeout(resolve, 5000));
      } else throw error;
    }
  }
}

// ✅ Fetch Shopify images from content files
async function fetchContentImages() {
  console.log("🚀 Fetching content images from files...");
  shopifyFiles = {};
  // Load existing cache (if exists)
  if (fs.existsSync(cacheFile)) {
    const cachedData = await fs.promises.readFile(cacheFile, "utf-8");
    shopifyFiles = JSON.parse(cachedData);
    console.log(
      `🔄 Loaded ${
        Object.keys(shopifyFiles).length
      } images from existing cache.`
    );
  }

  let hasNextPage = true;
  let endCursor = null;

  while (hasNextPage) {
    const query = `{
    files(first: 100, after: ${
      endCursor ? JSON.stringify(endCursor) : "null"
    }) {
        pageInfo { hasNextPage }
        edges {
          cursor
          node {
            ... on MediaImage {
              image { 
                id 
                url 
              }
            }
          }
        }
      }
    }`;

    const response = await safeShopifyQuery(query);
    const filesData = response.data?.data?.files?.edges || [];

    filesData.forEach(({ node, cursor }) => {
      const url = node.image?.url;
      if (!url) {
        console.warn("⚠️ Missing URL for files image:", node);
        return;
      }
      const fileName = url?.split("/").pop()?.split("?")[0];
      if (fileName && url) {
        shopifyFiles[fileName] = url;
      }
      endCursor = cursor;
    });

    hasNextPage = response.data?.data?.files?.pageInfo?.hasNextPage || false;
    console.log(
      `✅ Collected ${Object.keys(shopifyFiles).length} content images`
    );
  }
  // Save updated cache
  await fs.promises.writeFile(cacheFile, JSON.stringify(shopifyFiles, null, 2));
}

// ✅ Fetch images linked to all collections
async function fetchCollectionImages() {
  console.log("🚀 Fetching collection images...");
  shopifyFiles = {};
  // Load existing cache (if exists)
  if (fs.existsSync(cacheFile)) {
    const cachedData = await fs.promises.readFile(cacheFile, "utf-8");
    shopifyFiles = JSON.parse(cachedData);
    console.log(
      `🔄 Loaded ${
        Object.keys(shopifyFiles).length
      } images from existing cache.`
    );
  }

  let hasNextPage = true;
  let endCursor = null;

  while (hasNextPage) {
    const query = `{
      collections(first: 100, after: ${
        endCursor ? JSON.stringify(endCursor) : "null"
      }) {
        pageInfo { hasNextPage }
        edges {
          cursor
          node {
            id
            image { id url }
          }
        }
      }
    }`;

    const response = await safeShopifyQuery(query);
    const collectionsData = response.data?.data?.collections?.edges || [];

    collectionsData.forEach(({ node, cursor }) => {
      const url = node.image?.url;
      if (!url) {
        console.warn("⚠️ Missing URL for collection image:", node);
        return;
      }
      const fileName = url?.split("/").pop()?.split("?")[0];
      if (fileName && url) {
        const key = fileName.concat('_collection');
          shopifyFiles[key] = url;
      }
      endCursor = cursor;
    });

    hasNextPage =
      response.data?.data?.collections?.pageInfo?.hasNextPage || false;
    console.log(
      `✅ Collected ${Object.keys(shopifyFiles).length} collection images`
    );
  }
  // Save updated cache
  await fs.promises.writeFile(cacheFile, JSON.stringify(shopifyFiles, null, 2));
}

// ✅ Fetch images linked to all products
async function fetchProductImages() {
  console.log("🚀 Fetching product images...");
  shopifyFiles = {};
  // Load existing cache (if exists)
  if (fs.existsSync(cacheFile)) {
    const cachedData = await fs.promises.readFile(cacheFile, "utf-8");
    shopifyFiles = JSON.parse(cachedData);
    console.log(
      `🔄 Loaded ${
        Object.keys(shopifyFiles).length
      } images from existing cache.`
    );
  }

  let hasNextPage = true;
  let endCursor = null;

  while (hasNextPage) {
    const query = `{
      products(first: 100, after: ${
        endCursor ? JSON.stringify(endCursor) : "null"
      }) {
        pageInfo { hasNextPage }
        edges {
          cursor
          node {
            id
            images(first: 10) {
              edges {
                node { id url }
              }
            }
          }
        }
      }
    }`;

    const response = await safeShopifyQuery(query);
    const productsData = response.data?.data?.products?.edges || [];

    productsData.forEach(({ node, cursor }) => {
      node.images.edges.forEach(({ node: imageNode }) => {
        const url = imageNode.url;
        if (!url) {
          console.warn("⚠️ Missing URL for product image:", imageNode);
          return;
        }
        const fileName = url?.split("/").pop()?.split("?")[0];
        if (fileName && url) {
          const key = fileName.concat('_product');
          shopifyFiles[key] = url;
        }
      });
      endCursor = cursor;
    });

    hasNextPage = response.data?.data?.products?.pageInfo?.hasNextPage || false;
    console.log(
      `✅ Collected ${Object.keys(shopifyFiles).length} product images`
    );
  }
  // Save updated cache
  await fs.promises.writeFile(cacheFile, JSON.stringify(shopifyFiles, null, 2));
}

// ✅ Fetch all Shopify images & save to cache
async function fetchShopifyFiles() {
  console.log("🚀 Fetching all Shopify images...");

  shopifyFiles = {}; // Reset before fetching

  // Fetch and combine content images
  await fetchContentImages();

  // Fetch and combine collection images
  await fetchCollectionImages();

  // Fetch and combine product images
  await fetchProductImages();
}

// ✅ Generate Alt Text using OpenAI with rate limit handling
async function generateAltText(imageUrl) {
  while (true) {
    try {
      console.log(`📤 Sending request to OpenAI for: ${imageUrl}`);
      const response = await axios.post(
        "https://api.openai.com/v1/chat/completions",
        {
          model: "gpt-4-turbo",
          messages: [
            {
              role: "user",
              content: [
                {
                  type: "text",
                  text: "Generate a short, descriptive alt text for this image.",
                },
                { type: "image_url", image_url: { url: imageUrl } },
              ],
            },
          ],
          max_tokens: 100,
        },
        { headers: { Authorization: `Bearer ${OPENAI_API_KEY}` } }
      );
      console.log(
        `📥 OpenAI Response: ${JSON.stringify(response.data, null, 2)}`
      );
      return (
        response.data.choices[0]?.message?.content?.trim() || "Empty Alt Text"
      );
    } catch (error) {
      console.error(
        `❌ Error generating alt text for ${imageUrl}:`,
        error.response?.data || error.message
      );
      if (error.response?.data?.error?.code === "rate_limit_exceeded") {
        console.log(
          `⏳ OpenAI rate limit exceeded. Retrying in ${
            openAiRequestDelay / 1000
          } seconds...`
        );
        await new Promise((resolve) => setTimeout(resolve, openAiRequestDelay));
      } else {
        return "Error generating alt text";
      }
    }
  }
}

// ✅ Process CSV in chunks with controlled OpenAI chunked requests
async function processCsv() {
  console.log("📂 Starting CSV processing...");
  const csvWriter = createCsvWriter({
    path: outputCsv,
    header: [
      { id: "ID", title: "ID" },
      { id: "File Name", title: "File Name" },
      { id: "Command", title: "Command" },
      { id: "Link", title: "Link" },
      { id: "Alt Text", title: "Alt Text" },
      { id: "Created At", title: "Created At" },
      { id: "Type", title: "Type" },
      { id: "Mime Type", title: "Mime Type" },
      { id: "Width", title: "Width" },
      { id: "Height", title: "Height" },
      { id: "Duration", title: "Duration" },
      { id: "Status", title: "Status" },
      { id: "Errors", title: "Errors" },
    ],
  });

  const stream = fs.createReadStream(inputCsv).pipe(csv());
  let batch = [];

  stream.on("data", (row) => {
    console.log(`📥 Read row from CSV: ${JSON.stringify(row)}`);
    batch.push(row);
  });

  stream.on("end", async () => {
    console.log(`✅ Finished reading CSV. Total rows: ${batch.length}`);
    for (let i = 0; i < batch.length; i += batchSize) {
      const batchSlice = batch.slice(i, i + batchSize);
      console.log(`🔄 Processing batch from row ${i + 1} to ${i + batchSize}`);
      await processBatch(batchSlice, csvWriter);
    }
    console.log("✅ Finished processing all CSV rows");
  });
}

async function processBatch(rows, csvWriter) {
  console.log(`🔄 Processing batch with ${rows.length} rows`);

  for (let i = 0; i < rows.length; i += openAiChunkSize) {
    const chunk = rows.slice(i, i + openAiChunkSize);
    console.log(`📦 Processing OpenAI chunk of ${chunk.length} rows`);
    
    for (const row of chunk) {
      let fileName = row["File Name"]?.trim();

      if (!fileName) {
        console.warn(`⚠️ Skipping row ${i + 1}: No file name found.`);
        continue;
      }

      // ✅ Try to find exact match first
      let matchedFile = shopifyFiles[fileName];

      if (!matchedFile) {
        // ✅ Remove "_product" or "_collection" suffix before searching
        const cleanedFileName = fileName.replace(/(_product|_collection)$/, "");
        matchedFile = shopifyFiles[cleanedFileName];

        if (matchedFile) {
          console.log(`🔄 Adjusted file name match: ${fileName} → ${cleanedFileName}`);
        }
      }

      if (!matchedFile) {
        console.warn(`⚠️ Skipping row ${i + 1}: No matching Shopify file for '${fileName}'`);
        continue;
      }

      row["Updated Link"] = matchedFile;
      console.log(`🔗 Found file match for ${fileName}: ${row["Updated Link"]}`);

      if (!row["Alt Text"] && row["Updated Link"]) {
        console.log(`📤 Calling generateAltText() for: ${row["Updated Link"]}`);
        row["Alt Text"] = await generateAltText(row["Updated Link"]);
      }
    }

    console.log(`✅ Finished processing OpenAI chunk of ${chunk.length} rows`);
    await new Promise((resolve) => setTimeout(resolve, openAiChunkDelay));
  }

  await csvWriter.writeRecords(
    rows.map(({ "Updated Link": _, ...rest }) => rest)
  );
  console.log(`✅ Processed and saved batch of ${rows.length} rows`);
}

// ✅ Execute everything
(async () => {
  console.log("🚀 Starting full execution...");
  if (fs.existsSync(cacheFile)) {
    shopifyFiles = JSON.parse(fs.readFileSync(cacheFile));
    console.log("✅ Loaded Shopify files from cache");
  } else {
    await fetchShopifyFiles();
  }
  await processCsv();
})();
