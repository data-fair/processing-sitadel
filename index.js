const download = require('./src/download')
const process = require('./src/process')
const upload = require('./src/upload')
const fs = require('fs-extra')
const path = require('path')

exports.run = async ({ pluginConfig, processingConfig, tmpDir, axios, log, patchConfig }) => {
  await log.step('Configuration')
  await log.info(`Url dataset information parcelles : ${processingConfig.urlParcelData.href}`)
  await log.info(`Fichier à traiter : ${processingConfig.processFile}`)
  await log.info(`Supprimer les fichiers téléchargés : ${processingConfig.clearFiles}`)
  await log.info(`Mise à jour forcée : ${processingConfig.forceUpdate}`)
  await log.info(`Limite URL : ${pluginConfig.urlLimit ? pluginConfig.urlLimit : 2000}`)

  await download(processingConfig, tmpDir, axios, log)

  if (processingConfig.datasetMode === 'update' && !processingConfig.forceUpdate) {
    try {
      await log.step('Vérification de l\'en-tête du jeu de données')
      const schemaActuelDataset = (await axios.get(`api/v1/datasets/${processingConfig.dataset.id}/schema`, { params: { calculated: false } })).data.map((elem) => `"${elem.key}"`).join(';')

      let files = await fs.readdir(tmpDir)
      files = files.filter(file => file.endsWith('.csv') && file.includes(processingConfig.processFile) && !file.startsWith('sitadel'))
      const file = files[0] && path.join(tmpDir, files[0])
      const headFile = fs.createReadStream(file, { encoding: 'utf8' })
      let head

      await new Promise((resolve) => {
        headFile.once('data', (chunk) => {
          head = chunk.slice(0, chunk.indexOf('\n'))
          resolve()
        })
      })

      if (!head.includes(schemaActuelDataset.slice(0, head.length - 1))) {
        await log.info('Le jeu de données ne possède pas la même en-tête que le fichier téléchargé. Activez la mise à jour forcée pour mettre à jour')
        throw new Error('En-têtes différentes entre les fichiers')
      } else {
        await log.info('En-têtes identiques, mise à jour')
      }
    } catch (err) {
      await log.info(err)
      throw err
    }
  }

  await process(pluginConfig, processingConfig, tmpDir, axios, log)
  await upload(processingConfig, tmpDir, axios, log, patchConfig)
  if (processingConfig.clearFiles) {
    await fs.emptyDir(tmpDir)
  }
}
