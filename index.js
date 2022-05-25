const download = require('./src/download')
const process = require('./src/process')
const upload = require('./src/upload')
const fs = require('fs-extra')

exports.run = async ({ pluginConfig, processingConfig, tmpDir, axios, log, patchConfig }) => {
  await log.step('Configuration')
  await log.info(`Url dataset information parcelles : ${processingConfig.urlParcelData.href}`)
  await log.info(`Fichier à traiter : ${processingConfig.processFile}`)
  await log.info(`Supprimer les fichiers téléchargés : ${processingConfig.clearFiles}`)
  await log.info(`Limite URL : ${pluginConfig.urlLimit}`)
  await download(processingConfig, tmpDir, axios, log)
  await process(pluginConfig, processingConfig, tmpDir, axios, log)
  await upload(processingConfig, tmpDir, axios, log, patchConfig)
  if (processingConfig.clearFiles) {
    await fs.emptyDir(tmpDir)
  }
}
