const { defineConfig } = require('@playwright/test');
const path = require('node:path');

const url = 'http://127.0.0.1:5189';

module.exports = defineConfig({
    testDir: '.',
    use: { baseURL: url },
    webServer: {
        command: `dotnet run --project ServiceBusDemo.Web --no-launch-profile -- --urls ${url}`,
        cwd: path.resolve(__dirname, '../..'),
        url,
        timeout: 120_000
    }
});
