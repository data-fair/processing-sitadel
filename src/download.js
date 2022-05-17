const fs = require('fs-extra')
const path = require('path')
const util = require('util')
const pump = util.promisify(require('pump'))
const exec = util.promisify(require('child_process').exec)

const withStreamableFile = async (filePath, fn) => {
  // creating empty file before streaming seems to fix some weird bugs with NFS
  await fs.ensureFile(filePath + '.tmp')
  await fn(fs.createWriteStream(filePath + '.tmp'))
  // Try to prevent weird bug with NFS by forcing syncing file before reading it
  const fd = await fs.open(filePath + '.tmp', 'r')
  await fs.fsync(fd)
  await fs.close(fd)
  // write in tmp file then move it for a safer operation that doesn't create partial files
  await fs.move(filePath + '.tmp', filePath, { overwrite: true })
}

module.exports = async (processingConfig, dir = 'data', axios, log) => {
  const datasetId = '5a5f4f6c88ee387da4d252a3'
  const res = await axios.get('https://www.data.gouv.fr/api/1/datasets/' + datasetId + '/')

  const ressources = res.data.resources
  const processingFile = processingConfig.processFile
  await log.step('Téléchargement')
  for (const file of ressources) {
    if (file.type === 'main' && file.format !== 'html' && (file.title.normalize('NFD').replace(/[\u0300-\u036f]/g, '')).includes(processingFile)) {
      const url = new URL(file.url)
      const filePath = `${dir}/${path.parse(url.pathname).base}`
      await log.info(`téléchargement du fichier ${file.title}, écriture dans ${filePath}`)
      await withStreamableFile(filePath, async (writeStream) => {
        const res = await axios({ url: url.href, method: 'GET', responseType: 'stream' })
        await pump(res.data, writeStream)
      })

      if (filePath.endsWith('.zip')) {
        await log.info(`extraction de l'archive ${filePath}`, '')
        const { stderr } = await exec(`unzip -o ${filePath}`)
        if (stderr) throw new Error(`échec à l'extraction de l'archive ${filePath} : ${stderr}`)
        await fs.remove(filePath)
        const files = await fs.readdir(dir)
        await log.info(`Contenu répertoire de travail ${dir} après extraction : ${files}`)
      }
    }
  }
}
