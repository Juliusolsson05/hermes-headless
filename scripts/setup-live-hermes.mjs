// Prepare the Hermes runtime the live tier and the recorder boot.
//
// WHY not `pip install hermes-agent`: Hermes deliberately fails wheel builds
// outside its own package manager (setup.py refuses; it is run from source).
// So this archives a pinned source tree, installs ONLY its locked dependency
// set (uv.lock → requirements) into a private venv, and the harness runs
// `python -m hermes_cli.main` with the source on PYTHONPATH.
//
// Source: HERMES_SOURCE, else the agent-code reference checkout
// (`../../vendor/hermes-agent-src` when this package is the app's submodule).
// Output: `.cache/hermes-runtime/{src,venv}` (git-ignored). Needs `uv` and a
// Python that satisfies Hermes' requires-python (3.14 is what the lock pins
// its core dependency markers to).

import { execFileSync } from 'node:child_process'
import { existsSync, mkdirSync, rmSync, writeFileSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = resolve(fileURLToPath(new URL('..', import.meta.url)))
const source = resolve(process.env.HERMES_SOURCE ?? join(root, '..', '..', 'vendor', 'hermes-agent-src'))
const runtime = resolve(process.env.HERMES_LIVE_RUNTIME ?? join(root, '.cache', 'hermes-runtime'))
const python = process.env.HERMES_LIVE_PYTHON ?? '3.14'

if (!existsSync(join(source, 'hermes_cli'))) {
  console.error(`Hermes source not found at ${source}. Set HERMES_SOURCE or check out vendor/hermes-agent-src.`)
  process.exit(1)
}

rmSync(runtime, { recursive: true, force: true })
mkdirSync(join(runtime, 'src'), { recursive: true })
// `git archive` exports exactly the committed tree: no stray build output
// from the reference checkout can leak into the runtime.
execFileSync('sh', ['-c', `git -C "${source}" archive HEAD | tar -x -C "${join(runtime, 'src')}"`], { stdio: 'inherit' })
const requirements = execFileSync(
  'uv',
  ['export', '--frozen', '--no-hashes', '--no-emit-project', '--no-dev', '--python', python],
  { cwd: join(runtime, 'src'), encoding: 'utf8' },
)
writeFileSync(join(runtime, 'requirements.txt'), requirements)
execFileSync('uv', ['venv', '--quiet', '--python', python, join(runtime, 'venv')], { stdio: 'inherit' })
execFileSync('uv', ['pip', 'install', '--quiet', '--python', join(runtime, 'venv', 'bin', 'python'), '-r', join(runtime, 'requirements.txt')], { stdio: 'inherit' })
const commit = execFileSync('git', ['-C', source, 'rev-parse', 'HEAD'], { encoding: 'utf8' }).trim()
writeFileSync(join(runtime, 'SOURCE_COMMIT'), `${commit}\n`)
console.log(`Hermes runtime ready at ${runtime} (source ${commit})`)
