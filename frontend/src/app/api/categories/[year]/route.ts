import { NextResponse } from 'next/server'
import pool from '@/lib/db'

export async function GET(
  _request: Request,
  { params }: { params: { year: string } }
) {
  try {
    const year = parseInt(params.year)
    const [rows] = await pool.execute(
      'SELECT DISTINCT id, name, year, legacy_id FROM categories_general WHERE year = ? ORDER BY name',
      [year]
    )
    return NextResponse.json(rows)
  } catch (error) {
    console.error('Error fetching categories:', error)
    return NextResponse.json({ error: 'Failed to fetch categories' }, { status: 500 })
  }
}
