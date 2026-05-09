'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'

interface SidebarProps {
  events?: { id: number; name: string; circuit_name: string; date_start: string }[]
  year?: number
}

export default function Sidebar({ events = [], year }: SidebarProps) {
  const pathname = usePathname()

  return (
    <aside className="w-64 shrink-0 hidden lg:block">
      <div className="sticky top-20 bg-[#111111] border border-[#222222] rounded-xl p-4">
        <h3 className="text-[#888888] text-xs font-semibold uppercase tracking-widest mb-3">
          {year ? `${year} Calendar` : 'Calendar'}
        </h3>
        <div className="space-y-1">
          {events.length === 0 && (
            <p className="text-[#555555] text-sm">No events</p>
          )}
          {events.map((event) => {
            const href = `/race/${event.id}`
            const isActive = pathname === href
            return (
              <Link
                key={event.id}
                href={href}
                className={`flex flex-col px-3 py-2 rounded-lg text-sm transition-colors ${
                  isActive
                    ? 'bg-[#E8002D]/20 text-white border border-[#E8002D]/30'
                    : 'text-[#888888] hover:text-white hover:bg-[#1a1a1a]'
                }`}
              >
                <span className="font-medium truncate">{event.name}</span>
                <span className="text-xs text-[#555555] truncate">{event.circuit_name}</span>
              </Link>
            )
          })}
        </div>
      </div>
    </aside>
  )
}
