### Creating Collections 
db.createCollection("users")
db.createCollection("categories")
db.createCollection("products")
db.createCollection("transactions")

## verifying
show collections

## i imported the data in mongodb 

## verifying the imported data
use ecommerce_bigdata
db.users.countDocuments()
db.categories.countDocuments()
db.products.countDocuments()
db.transactions.countDocuments()

## topselling products by revenue
db.transactions.aggregate([
  { $unwind: "$items" },

  {
    $group: {
      _id: "$items.product_id",
      totalRevenue: { $sum: "$items.subtotal" },
      totalQuantitySold: { $sum: "$items.quantity" }
    }
  },

  { $sort: { totalRevenue: -1 } },

  { $limit: 10 },

  {
    $lookup: {
      from: "products",
      localField: "_id",
      foreignField: "_id",
      as: "product_info"
    }
  },

  { $unwind: "$product_info" },

  {
    $project: {
      _id: 0,
      product_id: "$_id",
      product_name: "$product_info.name",
      totalRevenue: 1,
      totalQuantitySold: 1
    }
  }
])
