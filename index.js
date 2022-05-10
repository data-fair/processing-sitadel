const download = require('./src/download')
const process = require('./src/process')
const upload = require('./src/upload')
const fs = require('fs-extra')

exports.run = async ({ pluginConfig, processingConfig, tmpDir, axios, log, patchConfig }) => {
  await download(processingConfig, tmpDir, axios, log)
  await process(processingConfig, axios, log)
  await upload(processingConfig, tmpDir, axios, log, patchConfig)
  if (processingConfig.clearFiles) {
    await fs.emptyDir('./')
  }
}
