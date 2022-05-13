const download = require('./src/download')
const process = require('./src/process')
const upload = require('./src/upload')
const fs = require('fs-extra')

exports.run = async ({ pluginConfig, processingConfig, tmpDir, axios, log, patchConfig }) => {
  log.step('Configuration')
  log.info(`Url dataset information parcelles : ${processingConfig.urlParcelData.href}`)
  log.info(`Fichier à traiter : ${processingConfig.processFile}`)
  log.info(`Supprimer les fichiers téléchargés : ${processingConfig.clearFiles}`)
  await download(processingConfig, tmpDir, axios, log)
  await process(processingConfig, tmpDir, axios, log)
  await upload(processingConfig, tmpDir, axios, log, patchConfig)
  if (processingConfig.clearFiles) {
    await fs.emptyDir('./')
  }
}
