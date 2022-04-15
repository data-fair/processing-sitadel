// const fs = require('fs-extra')
// const path = require('path')
// const util = require('util')
// const exec = util.promisify(require('child_process').exec)
// const config = require('config')

module.exports = async (pluginConfig, dir = 'data', axios, log) => {
  const datasetId = '5a5f4f6c88ee387da4d252a3'
  const res = await axios.get('https://www.data.gouv.fr/api/1/datasets/' + datasetId)

  const ressources = res.data.resources

  for (const file of ressources) {
    if (file.type === 'main' && file.format !== 'html') {
      console.log(file.title)
      await axios.get(file.url)
    }
  }
}
