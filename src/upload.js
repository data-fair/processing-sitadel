const fs = require('fs-extra')
const path = require('path')
const util = require('util')
const FormData = require('form-data')

function displayBytes (aSize) {
  aSize = Math.abs(parseInt(aSize, 10))
  if (aSize === 0) return '0 octets'
  const def = [[1, 'octets'], [1000, 'ko'], [1000 * 1000, 'Mo'], [1000 * 1000 * 1000, 'Go'], [1000 * 1000 * 1000 * 1000, 'To'], [1000 * 1000 * 1000 * 1000 * 1000, 'Po']]
  for (let i = 0; i < def.length; i++) {
    if (aSize < def[i][0]) return (aSize / def[i - 1][0]).toLocaleString() + ' ' + def[i - 1][1]
  }
}

module.exports = async (processingConfig, tmpDir, axios, log, patchConfig) => {
  const datasetSchema = require(`./schema_${processingConfig.processFile}.json`)
  const formData = new FormData()

  if (processingConfig.datasetMode === 'update') {
    await log.step('Mise à jour du jeu de données')
    if (processingConfig.forceUpdate) {
      await log.info('Mise à jour du schéma')
      formData.append('schema', JSON.stringify(datasetSchema))
    }
  } else {
    formData.append('schema', JSON.stringify(datasetSchema))
    formData.append('title', processingConfig.dataset.title)
    await log.step('Création du jeu de données')
  }

  const filePath = path.join(tmpDir, 'sitadel-' + processingConfig.processFile + '.csv')
  formData.append('file', fs.createReadStream(filePath), { filename: path.parse(filePath).base })

  formData.getLength = util.promisify(formData.getLength)
  const contentLength = await formData.getLength()
  await log.info(`chargement de (${displayBytes(contentLength)})`)

  const dataset = (await axios({
    method: 'post',
    url: (processingConfig.dataset && processingConfig.dataset.id) ? `api/v1/datasets/${processingConfig.dataset.id}` : 'api/v1/datasets',
    data: formData,
    maxContentLength: Infinity,
    maxBodyLength: Infinity,
    headers: { ...formData.getHeaders(), 'content-length': contentLength }
  })).data

  if (processingConfig.datasetMode === 'update') {
    await log.info(`jeu de donnée mis à jour, id="${dataset.id}", title="${dataset.title}"`)
  } else {
    await log.info(`jeu de donnée créé, id="${dataset.id}", title="${dataset.title}"`)
    await patchConfig({ datasetMode: 'update', dataset: { id: dataset.id, title: dataset.title } })
  }
}
