import mysql from 'mysql2/promise'

const pool = mysql.createPool({
  host: process.env.DB_HOST,
  port: parseInt(process.env.DB_PORT || '3306'),
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  charset: process.env.DB_CHARSET || 'utf8mb4',
  waitForConnections: true,
  connectionLimit: 10,
})

export default pool
