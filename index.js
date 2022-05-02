const download = require('./src/download')
const process = require('./src/process')
const upload = require('./src/upload')

exports.run = async ({ pluginConfig, processingConfig, tmpDir, axios, log, patchConfig }) => {
  // await download(pluginConfig, tmpDir, axios, log)
  await process(processingConfig, axios, log)
  // if (!processingConfig.skipUpload) await upload(processingConfig, tmpDir, axios, log, patchConfig)
}
