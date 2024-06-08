const express = require("express");
const { Order, Product, sequelize } = require("./schema");
const { Kafka } = require("kafkajs");

const app = express();

// body parswe
app.use(express.json());
const port = 8000;

const kafka = new Kafka({
   clientId: 'my-app',
   // kafka:9092
   brokers: ['localhost:9092', 'localhost:9092']
})

const producer = kafka.producer()

// API create product
app.post("/api/create-product", async (req, res) => {
   const productData = req.body;
   try {
      const result = await Product.create(productData);
      res.json(result);
   } catch (error) {
      res.status(500).json({ error });
   }
});

// API placeholder
app.post("/api/placeorder", async (req, res) => {
   const { userId, productId } = req.body;
   try {
      const product = await Product.findOne({
         where: {
            id: productId,
         },
      });

      // TODO: ถ้า order หายไป amount ที่ถูกลบไปจะไม่กลับมา => sql transaction
      if (product.amount <= 0) {
         res.status(404).json({
            message: "out of product",
         });

         return false // เพื่อ break function ไม่ให้ไปทำอย่างอื่นต่อ
      }

      // update product amount
      product.amount -= 1;
      await product.save();
      
      // create order
      const order = await Order.create({
         userLineUid: userId,
         status: 'pending',
         productId,
      });

      const orderData = {
         productName: product.name,
         userLineUid: userId,
         orderId: order.id
      }
      // producer
      await producer.send({
         topic: 'message-order',
         messages: [
            {
               value: JSON.stringify(orderData)
            }
         ]
      })

      res.json({
         message: 'create order successful',
         order: order,
      });


   } catch (error) {
      // In case order error
      // product.amount += 1;
      // await product.save();
      res.status(500).json({ error });
   }
});



app.listen(port, async () => {
   await sequelize.sync();
   await producer.connect()
   console.log(`Express app listening at ${port}`);
});
