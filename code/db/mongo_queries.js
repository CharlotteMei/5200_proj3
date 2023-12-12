const { MongoClient, ObjectId } = require('mongodb');
const { createClient } = require('redis');
let redisClient = null;
// Connection URL
const url = 'mongodb://localhost:27017';

// Database Name
const dbName = '5200_proj2';

// Function to connect to MongoDB
async function connectToDatabase() {
    const client = new MongoClient(url);
    await client.connect();
    return client.db(dbName);
}

// Function to CRUD products
async function createProduct(product) {
    const database = await connectToDatabase();
    const result = await database.collection('Product').insertOne(product);
    console.log("[DB] Create result = ", result);

    // Connect to Redis
    redisClient = await createClient()
        .on("error", (err) => console.log("Redis Client connection error " + err))
        .connect();
    console.log("Connected to Redis Client");

    // Set the count of the product to 0
    // Check if _id exists and is an object
    if (result && result.insertedId && result.insertedId instanceof ObjectId) {
        const idString = result.insertedId.toString();
        console.log("[DB] Creating product with id = ", idString);
        await redisClient.set(`OnRackProduct:${idString}`, 0);
        console.log("redis set done with product_id = ", idString, " and count = ", await redisClient.get(`OnRackProduct:${idString}`));

    } else {
        console.error("[DB] Unable to retrieve product ID from the result:", result);
    }


    return result;
}

async function readAllProducts() {
    const database = await connectToDatabase();
    const result = await database.collection('Product').find({}).toArray();
    console.log("[DB] Reading all products result [0] = ", result[0]);

    // Connect to Redis
    redisClient = await createClient()
        .on("error", (err) => console.log("Redis Client connection error " + err))
        .connect();

    // loop through all product and get the OnRack count
    for (let i = 0; i < result.length; i++) {
        const product = result[i];
        const count = await redisClient.get(`OnRackProduct:${product._id.toString()}`);
        product.on_rack_count = count;
    }

    console.log("[DB] Reading all products after redis result [-1] = ", result[result.length - 1]);
    return result;
}

async function readProduct(id) {
    const database = await connectToDatabase();
    const result = await database.collection('Product').findOne({ _id: new ObjectId(id) });
    return result;
}

async function updateProduct(id, product) {
    const database = await connectToDatabase();
    console.log("[DB] Updating product with id = ", id, " and product = ", product);
    const result = await database.collection('Product').replaceOne({ _id: new ObjectId(id) }, product);
    console.log("[DB] Update result = ", result);
    return result;
}

async function deleteProduct(id) {
    console.log("[DB] Deleting product with id = ", id);
    const database = await connectToDatabase();
    const result = await database.collection('Product').deleteOne({ _id: new ObjectId(id) });
    return result;
}

// Function to CRUD rack items
async function readRack(userId) {
    const database = await connectToDatabase();
    const userCollection = database.collection('User');
    // query to get rack items for a given user
    const result = await userCollection.aggregate([
        {
            $match:
            /**
             * query: The query in MQL.
             */
            {
                "user.user_id": 2,
            },
        },
        {
            $unwind: "$rack",
        },
        {
            $lookup: {
                from: "Product",
                localField: "rack.product_id",
                foreignField: "_id",
                as: "products",
            },
        },
        {
            $unwind: "$products",
        },
        {
            $project:
            /**
             * specifications: The fields to
             *   include or exclude.
             */
            {
                rack: 1,
                products: 1,
            },
        },
    ]).toArray();
    console.log("[DB] Reading rack using userId", userId, " result = ", result);
    return result;
}

async function findUser(userId) {
    const database = await connectToDatabase();
    const userCollection = database.collection('User');
    // query to get rack items for a given user
    const result = await userCollection.aggregate([
        {
            $match:
            /**
             * query: The query in MQL.
             */
            {
                "user.user_id": parseInt(userId),
            },
        },]).toArray();
    console.log("[DB] Finding user using userId", userId, " result = ", result);
    return result;

}

async function addRackItem(userId, newRackItem) {
    // productId is the _id field for the product
    const database = await connectToDatabase();
    const userCollection = database.collection('User');
    // query to get rack items for a given user
    const user = await findUser(userId);

    if (user === null) {
        console.log("[DB] User not found for userId = ", userId);
        return null;
    }

    await redisClient.incr(`OnRackProduct:${newRackItem.product_id}`);
    console.log("redis incr done with product_id = ", newRackItem.product_id, " and count = ", await redisClient.get(`OnRackProduct:${newRackItem.product_id}`));

    const item = { product_id: new ObjectId(newRackItem._id), purchased_date: newRackItem.purchased_date };

    const result = await userCollection.updateOne(
        {
            "user.user_id": parseInt(userId),
        },
        {
            $push: {
                "rack": item
            }
        }
    );

    console.log("[DB] Adding rack item for userId = ", userId, " new rack item = ", item, " result = ", result)
    return result;
}

async function editRackItem(userId, productId, newRackItem) {
    // productId is the _id field for the product
    const database = await connectToDatabase();
    const userCollection = database.collection('User');
    // query to get rack items for a given user
    const user = await findUser(userId);

    if (user === null) {
        console.log("[DB] User not found for userId = ", userId);
        return null;
    }

    const result = await userCollection.updateOne(
        {
            "user.user_id": parseInt(userId),
            "rack.product_id": new ObjectId(productId)
        },
        {
            $set: {
                "rack.$.purchased_date": newRackItem.purchased_date
            }
        }
    );

    console.log("[DB] Editing rack item for userId = ", userId, " productId = ", productId, " new rack item = ", newRackItem, " result = ", result)
    return result;

}

async function deleteRackItem(userId, productId) {
    // productId is the _id field for the product
    const database = await connectToDatabase();
    const userCollection = database.collection('User');
    // query to get rack items for a given user
    const user = await findUser(userId);

    if (user === null) {
        console.log("[DB] User not found for userId = ", userId);
        return null;
    }

    const result = await userCollection.updateOne(
        {
            "user.user_id": parseInt(userId),
        },
        {
            $pull: {
                "rack": { "product_id": new ObjectId(productId) }
            }
        }
    );

    await redisClient.decr(`OnRackProduct:${productId}`);
    console.log("redis decr done with product_id = ", productId, " and count = ", await redisClient.get(`OnRackProduct:${productId}`));

    console.log("[DB] Deleting rack item for userId = ", userId, " productId = ", productId, " result = ", result)
    return result;

}


module.exports = {
    createProduct,
    readAllProducts,
    readProduct,
    updateProduct,
    deleteProduct,
    readRack,
    addRackItem,
    editRackItem,
    deleteRackItem
};