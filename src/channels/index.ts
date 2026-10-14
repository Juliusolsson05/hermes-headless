// The three channels every *-headless package exposes (claude-code-headless,
// codex-headless, opencode-headless, grok, pi): semantic (live turn), committed
// (durable rows), screen (activity). Same names, same role, so the app's
// provider session reads one shape across providers.

import { EventEmitter } from 'node:events'

import type { CommittedRow, SemanticEvent } from '../reconcile/ConversationReconciler.js'

export type ScreenActivityEvent = { active: boolean; status: string | null }

export class SemanticChannel extends EventEmitter {
  publish(event: SemanticEvent): void {
    this.emit('event', event)
  }
}

export type HistoryBoundary = { type: 'reset' | 'caught-up'; generation: number; reason?: 'snapshot' | 'new-conversation' | 'rewrite' }

export class CommittedChannel extends EventEmitter {
  publishRow(row: CommittedRow): void {
    this.emit('entry', row)
  }

  publishBoundary(boundary: HistoryBoundary): void {
    this.emit('boundary', boundary)
  }

  publishError(error: Error): void {
    this.emit('history_error', error)
  }
}

export class ScreenChannel extends EventEmitter {
  publishActivity(event: ScreenActivityEvent): void {
    this.emit('activity', event)
  }
}
