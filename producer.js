const { kafka } = require("./client")
const { Partitioners } = require('kafkajs')
const { stdin, stdout } = require("process")
const readline = require("readline")

const rl = readline.createInterface({
    input: stdin,
    output: stdout
})

async function init(){
    const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner })
    console.log("Connecting to producer...")
    await producer.connect()
    console.log("Producer connected!")

    rl.setPrompt(">")
    rl.prompt()
    
    rl.on("line", async function(line){
        const [ riderName, location ] = line.split(" ")
        await producer.send({
            topic: "rider_updates",
            messages: [
                {   
                    partition: location.toLowerCase() === "north" ? 0 : 1,
                    key: "location-update",
                    value: JSON.stringify({ name: riderName, loc: location}),
                },
            ],
        });
    }).on("close",async () => {
        console.log("Disconnecting producer")
        await producer.disconnect()
    })
}

init()