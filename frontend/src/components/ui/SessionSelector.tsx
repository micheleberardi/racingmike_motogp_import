'use client'

import type { Session } from '@/types'

interface SessionSelectorProps {
  sessions: Session[]
  selectedId: string | null
  onSelect: (id: string) => void
}

const SESSION_ORDER = ['FP1', 'FP2', 'FP3', 'FP4', 'Q1', 'Q2', 'SPR', 'RAC', 'WUP']

const SESSION_LABELS: Record<string, string> = {
  FP1: 'Free Practice 1',
  FP2: 'Free Practice 2',
  FP3: 'Free Practice 3',
  FP4: 'Free Practice 4',
  Q1: 'Qualifying 1',
  Q2: 'Qualifying 2',
  SPR: 'Sprint Race',
  RAC: 'Race',
  WUP: 'Warm Up',
}

function sortSessions(sessions: Session[]): Session[] {
  return [...sessions].sort((a, b) => {
    const ai = SESSION_ORDER.indexOf(a.type)
    const bi = SESSION_ORDER.indexOf(b.type)
    if (ai !== -1 && bi !== -1) return ai - bi
    if (ai !== -1) return -1
    if (bi !== -1) return 1
    return a.type.localeCompare(b.type)
  })
}

export default function SessionSelector({
  sessions,
  selectedId,
  onSelect,
}: SessionSelectorProps) {
  const sorted = sortSessions(sessions)

  return (
    <div className="flex flex-wrap gap-2">
      {sorted.map((session) => {
        const isSelected = session.id === selectedId
        const isCompleted = session.status?.toUpperCase() === 'FINISHED'
        return (
          <button
            key={session.id}
            onClick={() => onSelect(session.id)}
            className={`px-4 py-2 rounded-lg text-sm font-semibold border transition-all ${
              isSelected
                ? 'bg-[#E8002D] border-[#E8002D] text-white'
                : isCompleted
                ? 'bg-[#1a1a1a] border-[#333333] text-white hover:border-[#E8002D]/50 hover:text-[#E8002D]'
                : 'bg-[#111111] border-[#222222] text-[#555555] cursor-not-allowed'
            }`}
            disabled={!isCompleted && !isSelected}
            title={SESSION_LABELS[session.type] || session.type}
          >
            {session.type}
          </button>
        )
      })}
    </div>
  )
}
