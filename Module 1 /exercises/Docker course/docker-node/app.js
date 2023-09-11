const express = require('express')
const app = express()
const port = 3000

app.get('/', (request, response) => {
    response.send("01001000\n" +
        "01100101\n" +
        "01101100\n" +
        "01101100\n" +
        "01101111\n" +
        "00100001\n" +
        "00001010")
})

app.listen(port, () => {
    console.log('Port: ', port)
})
