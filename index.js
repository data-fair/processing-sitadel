const download = require('./src/download')
const processData = require('./src/process')
const upload = require('./src/upload')
const fetchParcels = require('./src/parcels')

exports.run = async ({ pluginConfig, processingConfig, tmpDir, axios, log, patchConfig }) => {
  await fetchParcels()
  await download(pluginConfig, tmpDir, axios, log)
  // await processData(tmpDir, log)
  // if (!processingConfig.skipUpload) await upload(processingConfig, tmpDir, axios, log, patchConfig)
}
