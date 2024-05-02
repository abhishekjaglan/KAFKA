const { kafka } = require("./client")

async function init(){
    const admin = kafka.admin()
    console.log("Conneting to admin...")
    await admin.connect()
    console.log("Connected to admin!")

    console.log("Creating topic [Riders Updates]")
    await admin.createTopics({
        topics: [
          {
            topic: "rider_updates",
            numPartitions: 2,
          },
        ],
      });
    console.log("Topic created...!")
    console.log("Disconnecting admin...")
    await admin.disconnect()
}

init();