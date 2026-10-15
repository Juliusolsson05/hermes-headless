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
import { lstatSync, renameSync, unlinkSync, writeFileSync } from 'node:fs'
import { dirname, join, basename } from 'node:path'
import { randomBytes } from 'node:crypto'

import { buildAuthorizeUrl, createPkce, exchangeCode, readAuthorizationCode } from '../src/auth/index.js'

const SIGN_IN_TIMEOUT_MS = 5 * 60_000

const [rawUrl, outFile] = process.argv.slice(2)
if (!rawUrl || !outFile) {
  console.error('usage: tsx research/deployment-sign-in.mts <hermes base url> <token file to write>')
  process.exit(2)
}
let baseUrl: string
try {
  const url = new URL(rawUrl)
  if ((url.protocol !== 'https:' && url.protocol !== 'http:') || url.username || url.password) throw new Error()
  baseUrl = `${url.origin}${url.pathname.replace(/\/+$/, '')}`
} catch {
  console.error('The base URL must be an http(s) URL without credentials, e.g. https://hermes.example.com')
  process.exit(2)
}
// Refuse to follow a symlink: writing "through" one would put a live
// session wherever it points, with that file's permissions (review).
try {
  if (lstatSync(outFile).isSymbolicLink()) {
    console.error(`${outFile} is a symlink; refusing to write tokens through it.`)
    process.exit(2)
  }
} catch {
  // Does not exist yet: fine.
}

const pkce = createPkce()
// Hermes accepts only a loopback IP literal as the redirect (auth/index.ts
// buildAuthorizeUrl). Port 0 lets the OS pick a free one.
const server = createServer()
await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
const port = (server.address() as { port: number }).port
const redirectUri = `http://127.0.0.1:${port}/callback`

const code = new Promise<string>((resolve, reject) => {
  const timer = setTimeout(() => reject(new Error('Sign-in did not finish within 5 minutes.')), SIGN_IN_TIMEOUT_MS)
  timer.unref()
  server.on('request', (req, res) => {
    if (!req.url?.startsWith('/callback')) {
      res.writeHead(404).end()
      return
    }
    const explicitError = new URL(req.url, 'http://127.0.0.1').searchParams.get('error')
    try {
      const value = readAuthorizationCode(req.url, pkce.state)
      res.writeHead(200, { 'content-type': 'text/plain' }).end('Signed in. You can close this tab.')
      clearTimeout(timer)
      resolve(value)
    } catch (error) {
      res.writeHead(400, { 'content-type': 'text/plain' }).end('That redirect was not for this sign-in.')
      // Only an explicit cancel ends the sign-in. A stray hit (a prefetch, a
      // replayed old redirect, a second tab) with the wrong state must not
      // kill the wait for the genuine one (review).
      if (explicitError) {
        clearTimeout(timer)
        reject(error)
      }
    }
  })
})

console.log(`Open this URL in your browser and sign in:\n\n${buildAuthorizeUrl({ baseUrl, codeChallenge: pkce.challenge, redirectUri, state: pkce.state })}\n`)
try {
  const tokens = await exchangeCode({ baseUrl, code: await code, codeVerifier: pkce.verifier })
  // A NEW file created 0600 and renamed over the target: `mode` on
  // writeFileSync only applies when it creates the file, so re-minting into
  // an existing 0644 file would have kept it world-readable (review).
  const temp = join(dirname(outFile), `.${basename(outFile)}.${randomBytes(6).toString('hex')}.tmp`)
  try {
    writeFileSync(temp, JSON.stringify(tokens, null, 2), { mode: 0o600, flag: 'wx' })
    renameSync(temp, outFile)
  } catch (error) {
    try { unlinkSync(temp) } catch { /* never created */ }
    throw error
  }
  console.log(`Wrote ${outFile} (mode 0600, expires ${new Date(tokens.expiresAt).toISOString()}).`)
} catch (error) {
  console.error(`Sign-in failed: ${error instanceof Error ? error.message : 'unknown error'}`)
  process.exitCode = 1
} finally {
  server.close()
}
