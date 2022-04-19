const childProcess = require('child_process')

module.exports = async function exec (command, cwd) {
  return new Promise((resolve, reject) => {
    childProcess.exec(command, { cwd: cwd || process.cwd() }, (err, stdout, stderr) => {
      if (stdout) console.log(stdout)
      if (stderr) console.log(stderr)

      if (err) reject(err)
      else resolve()
    })
  })
}
