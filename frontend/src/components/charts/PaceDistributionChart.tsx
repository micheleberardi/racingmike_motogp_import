'use client'

import ReactECharts from 'echarts-for-react'
import type { LapTime } from '@/types'

interface Props {
  laps: LapTime[]
  riders: string[]
}

function computeBoxPlot(values: number[]): [number, number, number, number, number] | null {
  if (values.length < 3) return null
  const sorted = [...values].sort((a, b) => a - b)
  const n = sorted.length
  const q1 = sorted[Math.floor(n * 0.25)]
  const median = sorted[Math.floor(n * 0.5)]
  const q3 = sorted[Math.floor(n * 0.75)]
  const iqr = q3 - q1
  const lower = Math.max(sorted[0], q1 - 1.5 * iqr)
  const upper = Math.min(sorted[n - 1], q3 + 1.5 * iqr)
  // echarts boxplot format: [min, Q1, median, Q3, max]
  return [lower, q1, median, q3, upper]
}

function msToStr(ms: number): string {
  const mins = Math.floor(ms / 60000)
  const secs = ((ms % 60000) / 1000).toFixed(3).padStart(6, '0')
  return mins > 0 ? `${mins}:${secs}` : `${secs}s`
}

export default function PaceDistributionChart({ laps, riders }: Props) {
  // Build per-rider lap times (exclude outliers > 115% median)
  const riderLapMs = new Map<string, number[]>()
  for (const rider of riders) {
    riderLapMs.set(rider, [])
  }

  for (const lap of laps) {
    if (lap.lap_time_ms > 0 && riderLapMs.has(lap.rider_name)) {
      riderLapMs.get(lap.rider_name)!.push(lap.lap_time_ms)
    }
  }

  // Filter outliers per rider
  const filteredRiderLapMs = new Map<string, number[]>()
  for (const [rider, times] of riderLapMs.entries()) {
    if (times.length === 0) continue
    const sorted = [...times].sort((a, b) => a - b)
    const mid = Math.floor(sorted.length / 2)
    const med = sorted.length % 2 !== 0 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2
    filteredRiderLapMs.set(rider, times.filter((t) => t <= med * 1.15))
  }

  // Sort riders by median
  const ridersSortedByMedian = [...filteredRiderLapMs.entries()]
    .map(([rider, times]) => {
      const sorted = [...times].sort((a, b) => a - b)
      const mid = Math.floor(sorted.length / 2)
      const med = sorted.length % 2 !== 0 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2
      return { rider, med }
    })
    .sort((a, b) => a.med - b.med)
    .map((x) => x.rider)

  const boxData = ridersSortedByMedian
    .map((rider) => {
      const times = filteredRiderLapMs.get(rider) || []
      return computeBoxPlot(times)
    })
    .filter(Boolean) as [number, number, number, number, number][]

  const validRiders = ridersSortedByMedian.filter((r) => {
    const times = filteredRiderLapMs.get(r) || []
    return times.length >= 3
  })

  // Scatter outlier points
  const outlierData: [number, number][] = []
  for (let i = 0; i < validRiders.length; i++) {
    const rider = validRiders[i]
    const times = filteredRiderLapMs.get(rider) || []
    const box = boxData[i]
    if (!box) continue
    for (const t of times) {
      if (t < box[0] || t > box[4]) {
        outlierData.push([i, t])
      }
    }
  }

  const option = {
    backgroundColor: '#111111',
    grid: { top: 20, right: 20, bottom: 100, left: 80 },
    xAxis: {
      type: 'category',
      data: validRiders,
      axisLabel: {
        color: '#888888',
        fontSize: 10,
        rotate: 45,
        interval: 0,
      },
      axisLine: { lineStyle: { color: '#333333' } },
    },
    yAxis: {
      type: 'value',
      axisLabel: {
        color: '#888888',
        fontSize: 11,
        formatter: (v: number) => msToStr(v),
      },
      axisLine: { lineStyle: { color: '#333333' } },
      splitLine: { lineStyle: { color: '#1a1a1a' } },
    },
    series: [
      {
        name: 'Pace',
        type: 'boxplot',
        data: boxData,
        itemStyle: {
          color: '#E8002D22',
          borderColor: '#E8002D',
          borderWidth: 2,
        },
        boxWidth: ['10%', '50%'],
      },
      {
        name: 'Outlier',
        type: 'scatter',
        data: outlierData,
        itemStyle: { color: '#E8002D', opacity: 0.6 },
        symbolSize: 5,
      },
    ],
    tooltip: {
      trigger: 'item',
      backgroundColor: '#1a1a1a',
      borderColor: '#333333',
      textStyle: { color: '#ffffff', fontSize: 12 },
      formatter: (params: { name: string; data: number[] | [number, number]; seriesName: string }) => {
        if (params.seriesName === 'Outlier') {
          const d = params.data as [number, number]
          return `<span style="color:#888">${validRiders[d[0]]}</span><br/>Outlier: ${msToStr(d[1])}`
        }
        const d = params.data as number[]
        return `<div style="font-weight:bold;margin-bottom:4px">${params.name}</div>
          Min: ${msToStr(d[0])}<br/>
          Q1: ${msToStr(d[1])}<br/>
          Median: ${msToStr(d[2])}<br/>
          Q3: ${msToStr(d[3])}<br/>
          Max: ${msToStr(d[4])}`
      },
    },
  }

  return (
    <ReactECharts
      option={option}
      style={{ height: '380px', width: '100%' }}
      opts={{ renderer: 'canvas' }}
    />
  )
}
