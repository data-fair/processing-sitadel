const fs = require('fs-extra')
const path = require('path')
const util = require('util')
const pump = util.promisify(require('pump'))
const exec = util.promisify(require('child_process').exec)
// const config = require('config')

// const { write } = require("fs")

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

module.exports = async (pluginConfig, dir = 'data', axios, log) => {
  const datasetId = '5a5f4f6c88ee387da4d252a3'
  const res = await axios.get('https://www.data.gouv.fr/api/1/datasets/' + datasetId)

  const ressources = res.data.resources
  for (const file of ressources) {
    if (file.type === 'main' && file.format !== 'html') {
      log.step(`téléchargement du fichier ${file.title}`)
      const url = new URL(file.url)
      const fileName = path.parse(url.pathname).base
      await withStreamableFile(fileName, async (writeStream) => {
        const res = await axios({ url: url.href, method: 'GET', responseType: 'stream' })
        await pump(res.data, writeStream)
      })

      if (fileName.endsWith('.zip')) {
        log.debug(`extraction de l'archive ${fileName}`, '')
        const { stderr } = await exec(`unzip -o ${fileName}`)
        if (stderr) throw new Error(`échec à l'extraction de l'archive ${fileName} : ${stderr}`)
      }
    }
  }
}
