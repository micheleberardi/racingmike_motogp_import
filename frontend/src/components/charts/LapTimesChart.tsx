'use client'

import ReactECharts from 'echarts-for-react'
import type { BestLap } from '@/types'

interface Props {
  bestLaps: BestLap[]
}

function msToStr(ms: number): string {
  if (!ms || ms <= 0) return '-'
  const mins = Math.floor(ms / 60000)
  const secs = ((ms % 60000) / 1000).toFixed(3).padStart(6, '0')
  return mins > 0 ? `${mins}:${secs}` : `${secs}s`
}

const SECTOR_COLORS = ['#E8002D', '#00A8E8', '#FFD700', '#00E496']

export default function LapTimesChart({ bestLaps }: Props) {
  // Best lap ranking - horizontal bar (top 15)
  const top15 = bestLaps.slice(0, 15)
  const bestMs = top15[0]?.lap_time_ms || 1

  const barOption = {
    backgroundColor: '#111111',
    grid: { top: 10, right: 80, bottom: 10, left: 150, containLabel: false },
    xAxis: {
      type: 'value',
      min: bestMs * 0.995,
      axisLabel: {
        color: '#888888',
        fontSize: 10,
        formatter: (v: number) => msToStr(v),
      },
      axisLine: { lineStyle: { color: '#333333' } },
      splitLine: { lineStyle: { color: '#1a1a1a' } },
    },
    yAxis: {
      type: 'category',
      data: top15.map((l, i) => `${i + 1}. ${l.rider_name}`),
      inverse: true,
      axisLabel: { color: '#cccccc', fontSize: 11 },
      axisLine: { lineStyle: { color: '#333333' } },
    },
    series: [
      {
        type: 'bar',
        data: top15.map((l, i) => ({
          value: l.lap_time_ms,
          itemStyle: {
            color: i === 0 ? '#FFD700' : i === 1 ? '#C0C0C0' : i === 2 ? '#CD7F32' : '#E8002D44',
            borderColor: i < 3 ? 'transparent' : '#E8002D',
            borderWidth: i < 3 ? 0 : 1,
          },
        })),
        label: {
          show: true,
          position: 'right',
          color: '#cccccc',
          fontSize: 11,
          formatter: (p: { value: number }) => msToStr(p.value),
        },
        barMaxWidth: 24,
      },
    ],
    tooltip: {
      trigger: 'axis',
      backgroundColor: '#1a1a1a',
      borderColor: '#333333',
      textStyle: { color: '#fff', fontSize: 12 },
      formatter: (params: { name: string; value: number }[]) => {
        if (!params.length) return ''
        const p = params[0]
        return `${p.name}<br/>${msToStr(p.value)}`
      },
    },
  }

  // Sector comparison chart
  const hasSectors = top15.some(
    (l) => l.t1_ms && l.t2_ms && l.t3_ms
  )

  const sectorRiders = top15.filter((l) => l.t1_ms && l.t2_ms && l.t3_ms).slice(0, 10)
  const sectors = ['T1', 'T2', 'T3', 'T4']
  const sectorKeys: (keyof BestLap)[] = ['t1_ms', 't2_ms', 't3_ms', 't4_ms']

  const sectorOption = {
    backgroundColor: '#111111',
    grid: { top: 30, right: 20, bottom: 80, left: 150 },
    xAxis: {
      type: 'value',
      axisLabel: {
        color: '#888888',
        fontSize: 10,
        formatter: (v: number) => `${(v / 1000).toFixed(1)}s`,
      },
      axisLine: { lineStyle: { color: '#333333' } },
      splitLine: { lineStyle: { color: '#1a1a1a' } },
    },
    yAxis: {
      type: 'category',
      data: sectorRiders.map((l) => l.rider_name),
      inverse: true,
      axisLabel: { color: '#cccccc', fontSize: 11 },
      axisLine: { lineStyle: { color: '#333333' } },
    },
    series: sectors.map((sector, si) => ({
      name: sector,
      type: 'bar',
      stack: 'sectors',
      data: sectorRiders.map((l) => {
        const val = l[sectorKeys[si]] as number | undefined
        return val || 0
      }),
      itemStyle: { color: SECTOR_COLORS[si] },
      label: si === 0 ? {
        show: false,
      } : { show: false },
    })),
    legend: {
      data: sectors,
      bottom: 0,
      textStyle: { color: '#888888' },
    },
    tooltip: {
      trigger: 'axis',
      backgroundColor: '#1a1a1a',
      borderColor: '#333333',
      textStyle: { color: '#fff', fontSize: 12 },
      formatter: (params: { seriesName: string; value: number; axisValue: string }[]) => {
        if (!params.length) return ''
        let html = `<div style="font-weight:bold;margin-bottom:4px">${params[0].axisValue}</div>`
        for (const p of params) {
          if (p.value > 0) {
            html += `<div>${p.seriesName}: ${msToStr(p.value)}</div>`
          }
        }
        return html
      },
    },
  }

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-white font-semibold mb-3">Best Lap Ranking</h3>
        <ReactECharts
          option={barOption}
          style={{ height: `${Math.max(200, top15.length * 28 + 40)}px`, width: '100%' }}
          opts={{ renderer: 'canvas' }}
        />
      </div>
      {hasSectors && sectorRiders.length > 0 && (
        <div>
          <h3 className="text-white font-semibold mb-3">Sector Times (Best Lap)</h3>
          <ReactECharts
            option={sectorOption}
            style={{ height: `${Math.max(200, sectorRiders.length * 28 + 80)}px`, width: '100%' }}
            opts={{ renderer: 'canvas' }}
          />
        </div>
      )}
    </div>
  )
}
