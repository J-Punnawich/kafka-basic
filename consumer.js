const { Kafka } = require("kafkajs");
const { Order } = require("./schema");
const axios = require("axios");
const { where } = require("sequelize");

require('dotenv').config()

const kafka = new Kafka({
   clientId: 'my-app',
   // kafka:9092
   brokers: ['localhost:9092', 'localhost:9092']
})

const consumer = kafka.consumer({ groupId: 'message-group' })

const run = async () => {
   await consumer.connect()
   await consumer.subscribe({
      topic: 'message-order',
      fromBeginning: true
   })

   // kafka feature ที่จะ clear ข้อมูลใน partition ถ้าไม่มี consumer ตัวไหนรับไป
   await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
         const messageData = JSON.parse(message.value.toString())
         console.log("=== consumer message", JSON.parse(message.value.toString()));


         const headers = {
            "Content-Type": "application/json",
            Authorization: `Bearer ${process.env.LINE_ACCESS_TOKEN}`,
         };

         const bodyMessage = {
            'to': messageData.userLineUid,
            'messages': [
               {
                  type: "text",
                  text: `Buy product ${messageData.productName}`
               }
            ]

         }

         try {
            // send line to user
            const res = await axios.post(process.env.LINE_API_URL,
               bodyMessage,
               { headers }
            )

            // update status order success
            await Order.update({
               status: "success"
            },
               {
                  where: {
                     id: messageData.orderId
                  }
               })


         } catch (error) {
            console.log('err', error)
         }
      }
   }
   )
}
// console เพราะ consumerไม่ได้รันบน express แต่ใช้ subscribe ช่วย
run().catch(console.error)
