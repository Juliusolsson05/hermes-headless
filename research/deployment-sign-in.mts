// Sign in to a deployed Hermes once and write a token file for the opt-in
// deployment smoke (src/HermesHeadless.deployment.live.test.ts, agent-code#1502).
//
//   npx tsx research/deployment-sign-in.mts https://hermes.example.com ~/.hermes-deployment-tokens.json
//
// WHY a separate sign-in instead of reading Agent Code's tokens: the app's
// tokens are encrypted with the OS keychain and rotate on refresh; a test that
// borrowed them could rotate the app's refresh token out from under it and
// sign the owner out. This file is the test's own session, minted through the
// same native PKCE flow the app uses, so it exercises the real route too.
//
// The password is typed into the server's own page in the system browser;
// this script never sees it. It only prints the URL to open.

import { createServer } from 'node:http'
import { writeFileSync } from 'node:fs'

import { buildAuthorizeUrl, createPkce, exchangeCode, readAuthorizationCode } from '../src/auth/index.js'

const [baseUrl, outFile] = process.argv.slice(2)
if (!baseUrl || !outFile) {
  console.error('usage: tsx research/deployment-sign-in.mts <hermes base url> <token file to write>')
  process.exit(2)
}

const pkce = createPkce()
// Hermes accepts only a loopback IP literal as the redirect (auth/index.ts
// buildAuthorizeUrl). Port 0 lets the OS pick a free one.
const server = createServer()
await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
const port = (server.address() as { port: number }).port
const redirectUri = `http://127.0.0.1:${port}/callback`

const code = new Promise<string>((resolve, reject) => {
  server.on('request', (req, res) => {
    if (!req.url?.startsWith('/callback')) {
      res.writeHead(404).end()
      return
    }
    try {
      const value = readAuthorizationCode(req.url, pkce.state)
      res.writeHead(200, { 'content-type': 'text/plain' }).end('Signed in. You can close this tab.')
      resolve(value)
    } catch (error) {
      res.writeHead(400, { 'content-type': 'text/plain' }).end('Sign-in failed; see the terminal.')
      reject(error)
    }
  })
})

console.log(`Open this URL in your browser and sign in:\n\n${buildAuthorizeUrl({ baseUrl, codeChallenge: pkce.challenge, redirectUri, state: pkce.state })}\n`)
try {
  const tokens = await exchangeCode({ baseUrl, code: await code, codeVerifier: pkce.verifier })
  // 0600: this file is a live session on the owner's server.
  writeFileSync(outFile, JSON.stringify(tokens, null, 2), { mode: 0o600 })
  console.log(`Wrote ${outFile} (expires ${new Date(tokens.expiresAt).toISOString()}).`)
} finally {
  server.close()
}
