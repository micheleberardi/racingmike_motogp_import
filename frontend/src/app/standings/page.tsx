'use client'

import { useEffect, useState } from 'react'
import StandingsChart from '@/components/charts/StandingsChart'
import { apiUrl } from '@/lib/api'
import type { Category, Standing } from '@/types'

function getPositionColor(pos: number) {
  if (pos === 1) return '#FFD700'
  if (pos === 2) return '#C0C0C0'
  if (pos === 3) return '#CD7F32'
  return '#555555'
}

export default function StandingsPage() {
  const [years, setYears] = useState<number[]>([])
  const [selectedYear, setSelectedYear] = useState<number | null>(null)
  const [categories, setCategories] = useState<Category[]>([])
  const [selectedCategory, setSelectedCategory] = useState<Category | null>(null)
  const [standings, setStandings] = useState<Standing[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    fetch(apiUrl('/api/years'))
      .then((r) => r.json())
      .then((data: number[]) => {
        const arr = Array.isArray(data) ? data : []
        setYears(arr)
        if (arr.length > 0) setSelectedYear(arr[0])
      })
      .catch(() => setError('Failed to load years'))
  }, [])

  useEffect(() => {
    if (!selectedYear) return
    fetch(apiUrl(`/api/categories/${selectedYear}`))
      .then((r) => r.json())
      .then((data: Category[]) => {
        const arr = Array.isArray(data) ? data : []
        setCategories(arr)
        const motogp = arr.find((c) => c.name.toLowerCase().includes('motogp'))
        setSelectedCategory(motogp || arr[0] || null)
      })
      .catch(() => {})
  }, [selectedYear])

  useEffect(() => {
    if (!selectedYear || !selectedCategory) return
    setLoading(true)
    setError(null)
    fetch(apiUrl(`/api/standings/${selectedYear}/${selectedCategory.id}`))
      .then((r) => r.json())
      .then((data: Standing[]) => {
        setStandings(Array.isArray(data) ? data : [])
        setLoading(false)
      })
      .catch(() => {
        setError('Failed to load standings')
        setLoading(false)
      })
  }, [selectedYear, selectedCategory])

  return (
    <div className="max-w-screen-2xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      {/* Header */}
      <div className="flex flex-wrap items-center gap-4 mb-8">
        <div>
          <h1 className="text-3xl font-extrabold text-white">
            Championship <span className="text-[#E8002D]">Standings</span>
          </h1>
          <p className="text-[#888888] text-sm mt-1">Full season points classification</p>
        </div>
        <div className="ml-auto flex items-center gap-3 flex-wrap">
          <select
            value={selectedYear || ''}
            onChange={(e) => setSelectedYear(parseInt(e.target.value))}
            className="bg-[#111111] border border-[#222222] text-white rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-[#E8002D]"
          >
            {years.map((y) => (
              <option key={y} value={y}>{y}</option>
            ))}
          </select>
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

      {error && (
        <div className="text-[#E8002D] mb-4 bg-[#E8002D]/10 border border-[#E8002D]/30 rounded-lg px-4 py-3">
          {error}
        </div>
      )}

      <div className="grid grid-cols-1 xl:grid-cols-5 gap-6">
        {/* Standings Table */}
        <div className="xl:col-span-3 bg-[#111111] border border-[#222222] rounded-xl overflow-hidden">
          <div className="p-5 border-b border-[#222222]">
            <h2 className="text-white font-bold text-lg">
              {selectedCategory?.name} — {selectedYear}
            </h2>
          </div>
          {loading ? (
            <div className="flex items-center justify-center h-48 text-[#555555]">
              Loading standings...
            </div>
          ) : standings.length === 0 ? (
            <div className="flex items-center justify-center h-48 text-[#555555]">
              No standings data available
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-[#222222]">
                    <th className="text-left text-[#888888] font-semibold px-5 py-3 w-12">POS</th>
                    <th className="text-left text-[#888888] font-semibold px-2 py-3 w-8">#</th>
                    <th className="text-left text-[#888888] font-semibold py-3">RIDER</th>
                    <th className="text-left text-[#888888] font-semibold py-3 hidden lg:table-cell">CONSTRUCTOR</th>
                    <th className="text-left text-[#888888] font-semibold py-3 hidden md:table-cell">TEAM</th>
                    <th className="text-right text-[#888888] font-semibold py-3 pr-5">PTS</th>
                  </tr>
                </thead>
                <tbody>
                  {standings.map((s) => (
                    <tr
                      key={s.rider_id}
                      className="border-b border-[#1a1a1a] hover:bg-[#1a1a1a] transition-colors"
                    >
                      <td className="px-5 py-3">
                        <span
                          className="inline-flex items-center justify-center w-7 h-7 rounded font-bold text-sm"
                          style={{
                            background: getPositionColor(s.position) + '22',
                            color: getPositionColor(s.position),
                          }}
                        >
                          {s.position}
                        </span>
                      </td>
                      <td className="px-2 py-3 text-[#555555] font-mono text-xs">{s.rider_number}</td>
                      <td className="py-3">
                        <div
                          style={{
                            borderLeft: s.team_color
                              ? `3px solid ${s.team_color}`
                              : '3px solid #333333',
                            paddingLeft: 10,
                          }}
                        >
                          <div className="text-white font-medium">{s.rider_full_name}</div>
                        </div>
                      </td>
                      <td className="py-3 text-[#888888] text-xs hidden lg:table-cell">
                        {s.constructor_name}
                      </td>
                      <td className="py-3 text-[#888888] text-xs hidden md:table-cell">
                        {s.team_name}
                      </td>
                      <td className="py-3 pr-5 text-right">
                        <span
                          className={`font-extrabold text-base ${
                            s.position === 1 ? 'text-[#FFD700]' : 'text-white'
                          }`}
                        >
                          {s.points}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>

        {/* Chart */}
        <div className="xl:col-span-2 bg-[#111111] border border-[#222222] rounded-xl p-5">
          <h2 className="text-white font-bold text-lg mb-4">Top 10 by Points</h2>
          {loading ? (
            <div className="flex items-center justify-center h-48 text-[#555555]">Loading...</div>
          ) : standings.length === 0 ? (
            <div className="flex items-center justify-center h-48 text-[#555555]">No data</div>
          ) : (
            <StandingsChart standings={standings} limit={10} />
          )}
        </div>
      </div>
    </div>
  )
}
