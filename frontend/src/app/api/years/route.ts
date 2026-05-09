import { NextResponse } from 'next/server'
import pool from '@/lib/db'

export async function GET() {
  try {
    const [rows] = await pool.execute(
      'SELECT DISTINCT year FROM events WHERE test = 0 ORDER BY year DESC'
    )
    const years = (rows as { year: number }[]).map((r) => r.year)
    return NextResponse.json(years)
  } catch (error) {
    console.error('Error fetching years:', error)
    return NextResponse.json({ error: 'Failed to fetch years' }, { status: 500 })
  }
}
