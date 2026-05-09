/** @type {import('next').NextConfig} */
const nextConfig = {
  basePath: process.env.NEXT_PUBLIC_BASE_PATH || '',
  experimental: {
    serverComponentsExternalPackages: ['mysql2'],
  },
}

module.exports = nextConfig
