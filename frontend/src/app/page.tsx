'use client'

import { useEffect, useState, useCallback } from 'react'
import Link from 'next/link'
import MetricCard from '@/components/ui/MetricCard'
import StandingsChart from '@/components/charts/StandingsChart'
import type { Category, Event, Standing, Result, Session } from '@/types'

function formatDate(dateStr: string) {
  if (!dateStr) return '-'
  const d = new Date(dateStr)
  return d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' })
}

function getPositionColor(pos: number | null) {
  if (pos === 1) return '#FFD700'
  if (pos === 2) return '#C0C0C0'
  if (pos === 3) return '#CD7F32'
  return '#555555'
}

export default function HomePage() {
  const [years, setYears] = useState<number[]>([])
  const [selectedYear, setSelectedYear] = useState<number | null>(null)
  const [categories, setCategories] = useState<Category[]>([])
  const [selectedCategory, setSelectedCategory] = useState<Category | null>(null)
  const [events, setEvents] = useState<Event[]>([])
  const [standings, setStandings] = useState<Standing[]>([])
  const [lastRaceResults, setLastRaceResults] = useState<Result[]>([])
  const [lastRaceEvent, setLastRaceEvent] = useState<Event | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    fetch('/api/years')
      .then((r) => r.json())
      .then((data: number[]) => {
        setYears(data)
        if (data.length > 0) setSelectedYear(data[0])
      })
      .catch(() => setError('Failed to load years'))
  }, [])

  useEffect(() => {
    if (!selectedYear) return
    setLoading(true)
    setCategories([])
    setSelectedCategory(null)
    fetch(`/api/categories/${selectedYear}`)
      .then((r) => r.json())
      .then((data: Category[]) => {
        setCategories(data)
        const motogp = data.find((c) => c.name.toLowerCase().includes('motogp'))
        setSelectedCategory(motogp || data[0] || null)
      })
      .catch(() => setError('Failed to load categories'))
  }, [selectedYear])

  useEffect(() => {
    if (!selectedYear) return
    fetch(`/api/events/${selectedYear}`)
      .then((r) => r.json())
      .then((data: Event[]) => setEvents(data))
      .catch(() => {})
  }, [selectedYear])

  const loadStandings = useCallback(() => {
    if (!selectedYear || !selectedCategory) return
    setLoading(true)
    fetch(`/api/standings/${selectedYear}/${selectedCategory.id}`)
      .then((r) => r.json())
      .then((data: Standing[]) => {
        setStandings(data)
        setLoading(false)
      })
      .catch(() => {
        setError('Failed to load standings')
        setLoading(false)
      })
  }, [selectedYear, selectedCategory])

  useEffect(() => {
    loadStandings()
  }, [loadStandings])

  // Find last completed race results
  useEffect(() => {
    if (!selectedYear || !selectedCategory || events.length === 0) return
    const today = new Date()
    const pastEvents = events
      .filter((e) => new Date(e.date_end) <= today)
      .sort((a, b) => new Date(b.date_end).getTime() - new Date(a.date_end).getTime())

    if (pastEvents.length === 0) return

    const lastEvent = pastEvents[0]
    setLastRaceEvent(lastEvent)

    // Get sessions for last event
    fetch(`/api/sessions/${selectedYear}/${lastEvent.id}/${selectedCategory.id}`)
      .then((r) => r.json())
      .then((sessions: Session[]) => {
        const raceSession = sessions.find((s) => s.type === 'RAC' && s.status === 'Finished')
        if (!raceSession) return
        return fetch(`/api/results/${raceSession.id}`)
      })
      .then((r) => r?.json())
      .then((results: Result[]) => {
        if (results) setLastRaceResults(results.slice(0, 10))
      })
      .catch(() => {})
  }, [selectedYear, selectedCategory, events])

  // Derived metrics
  const today = new Date()
  const completedRounds = events.filter((e) => new Date(e.date_end) <= today).length
  const nextEvent = events.find((e) => new Date(e.date_start) > today)
  const leader = standings[0]

  if (error) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-[#E8002D] text-lg">{error}</div>
      </div>
    )
  }

  return (
    <div className="max-w-screen-2xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      {/* Header / Selectors */}
      <div className="flex flex-wrap items-center gap-4 mb-8">
        <div>
          <h1 className="text-3xl font-extrabold text-white">
            MotoGP <span className="text-[#E8002D]">Dashboard</span>
          </h1>
          <p className="text-[#888888] text-sm mt-1">Season overview and championship standings</p>
        </div>
        <div className="ml-auto flex items-center gap-3 flex-wrap">
          {/* Year selector */}
          <select
            value={selectedYear || ''}
            onChange={(e) => setSelectedYear(parseInt(e.target.value))}
            className="bg-[#111111] border border-[#222222] text-white rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-[#E8002D] transition-colors"
          >
            {years.map((y) => (
              <option key={y} value={y}>{y}</option>
            ))}
          </select>
          {/* Category selector */}
          <div className="flex gap-1">
            {categories.map((cat) => (
              <button
                key={cat.id}
                onClick={() => setSelectedCategory(cat)}
                className={`px-3 py-2 rounded-lg text-sm font-semibold border transition-all ${
                  selectedCategory?.id === cat.id
                    ? 'bg-[#E8002D] border-[#E8002D] text-white'
                    : 'bg-[#111111] border-[#222222] text-[#888888] hover:text-white hover:border-[#E8002D]/50'
                }`}
              >
                {cat.name}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Metric Cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        <MetricCard
          title="Completed Rounds"
          value={completedRounds}
          subtitle={`of ${events.length} total`}
        />
        <MetricCard
          title="Next Race"
          value={nextEvent ? nextEvent.name : 'Season Over'}
          subtitle={nextEvent ? formatDate(nextEvent.date_start) : ''}
        />
        <MetricCard
          title="Championship Leader"
          value={leader ? leader.rider_full_name.split(' ').pop() || leader.rider_full_name : '-'}
          subtitle={leader ? leader.team_name : ''}
          accent
        />
        <MetricCard
          title="Leader Points"
          value={leader ? leader.points : '-'}
          subtitle={
            standings[1]
              ? `+${leader.points - standings[1].points} over ${standings[1].rider_full_name.split(' ').pop()}`
              : ''
          }
        />
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-5 gap-6">
        {/* Last Race Results */}
        <div className="xl:col-span-3 bg-[#111111] border border-[#222222] rounded-xl p-5">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h2 className="text-white font-bold text-lg">Last Race Results</h2>
              {lastRaceEvent && (
                <p className="text-[#888888] text-sm">
                  {lastRaceEvent.name} — {lastRaceEvent.circuit_name}
                </p>
              )}
            </div>
            {lastRaceEvent && (
              <Link
                href={`/race/${lastRaceEvent.id}`}
                className="text-[#E8002D] text-sm hover:underline"
              >
                Full results →
              </Link>
            )}
          </div>
          {lastRaceResults.length === 0 ? (
            <div className="flex items-center justify-center h-32 text-[#555555]">
              {loading ? 'Loading...' : 'No results available'}
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-[#222222]">
                    <th className="text-left text-[#888888] font-semibold pb-2 pr-3 w-8">POS</th>
                    <th className="text-left text-[#888888] font-semibold pb-2 pr-3">#</th>
                    <th className="text-left text-[#888888] font-semibold pb-2">RIDER</th>
                    <th className="text-left text-[#888888] font-semibold pb-2 hidden md:table-cell">TEAM</th>
                    <th className="text-right text-[#888888] font-semibold pb-2">PTS</th>
                    <th className="text-right text-[#888888] font-semibold pb-2 hidden sm:table-cell">GAP</th>
                  </tr>
                </thead>
                <tbody>
                  {lastRaceResults.map((result, idx) => (
                    <tr
                      key={idx}
                      className="border-b border-[#1a1a1a] hover:bg-[#1a1a1a] transition-colors"
                    >
                      <td className="py-2 pr-3">
                        <span
                          className="inline-flex items-center justify-center w-6 h-6 rounded text-xs font-bold"
                          style={{
                            background: getPositionColor(result.position) + '22',
                            color: getPositionColor(result.position),
                          }}
                        >
                          {result.position ?? '-'}
                        </span>
                      </td>
                      <td className="py-2 pr-3 text-[#555555] font-mono text-xs">
                        {result.rider_number}
                      </td>
                      <td className="py-2">
                        <span
                          style={{
                            borderLeft: result.team_color
                              ? `3px solid ${result.team_color}`
                              : '3px solid #333',
                            paddingLeft: 8,
                          }}
                        >
                          {result.rider_full_name}
                        </span>
                      </td>
                      <td className="py-2 text-[#888888] hidden md:table-cell text-xs">
                        {result.team_name}
                      </td>
                      <td className="py-2 text-right font-bold text-[#E8002D]">
                        {result.points || 0}
                      </td>
                      <td className="py-2 text-right text-[#888888] text-xs hidden sm:table-cell">
                        {result.gap_first || '-'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>

        {/* Mini Standings Chart */}
        <div className="xl:col-span-2 bg-[#111111] border border-[#222222] rounded-xl p-5">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-white font-bold text-lg">Championship Top 5</h2>
            <Link href="/standings" className="text-[#E8002D] text-sm hover:underline">
              Full standings →
            </Link>
          </div>
          {loading ? (
            <div className="flex items-center justify-center h-32 text-[#555555]">Loading...</div>
          ) : standings.length === 0 ? (
            <div className="flex items-center justify-center h-32 text-[#555555]">No data</div>
          ) : (
            <StandingsChart standings={standings} limit={5} />
          )}
        </div>
      </div>

      {/* Season Calendar */}
      {events.length > 0 && (
        <div className="mt-6 bg-[#111111] border border-[#222222] rounded-xl p-5">
          <h2 className="text-white font-bold text-lg mb-4">Season Calendar</h2>
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-3">
            {events.map((event) => {
              const isPast = new Date(event.date_end) < today
              const isNext = nextEvent?.id === event.id
              return (
                <Link
                  key={event.id}
                  href={`/race/${event.id}`}
                  className={`rounded-lg p-3 border transition-all hover:scale-105 ${
                    isNext
                      ? 'border-[#E8002D] bg-[#E8002D]/10'
                      : isPast
                      ? 'border-[#222222] bg-[#0f0f0f] hover:border-[#333333]'
                      : 'border-[#222222] bg-[#111111] opacity-60 hover:opacity-80'
                  }`}
                >
                  <div
                    className={`text-xs font-semibold mb-1 ${
                      isNext ? 'text-[#E8002D]' : isPast ? 'text-[#888888]' : 'text-[#555555]'
                    }`}
                  >
                    {formatDate(event.date_start)}
                  </div>
                  <div className="text-white text-xs font-bold leading-tight truncate">
                    {event.country_name}
                  </div>
                  <div className="text-[#555555] text-xs truncate mt-0.5">{event.circuit_name}</div>
                </Link>
              )
            })}
          </div>
        </div>
      )}
    </div>
  )
}
