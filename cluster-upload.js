/**
 * Upload a set of files to a directory in IPFS Cluster.
 *
 * Usage:
 *     node cluster-upload.js path/to/file0 [...files]
 */
import { CID } from 'multiformats'
import { CarWriter, CarReader } from '@ipld/car'
import * as dagCbor from '@ipld/dag-cbor'
import { garbage } from 'ipld-garbage'
import { sha256 } from 'multiformats/hashes/sha2'
import { TreewalkCarSplitter } from 'carbites'

import dotenv from 'dotenv'
import { Cluster } from '@nftstorage/ipfs-cluster'
import fetch from '@web-std/fetch'
import { FormData } from '@web-std/form-data'
import { File, Blob } from '@web-std/file'

Object.assign(global, { fetch, File, Blob, FormData })

dotenv.config()

async function main () {
  if (!process.env.CLUSTER_URL) {
    throw new Error('missing IPFS Cluster URL')
  }

  console.log(`🔌 Using IPFS Cluster URL: ${process.env.CLUSTER_URL}`)

  const targetSize = 1024 * 1024 * 110 // ~110MB CARs
  const carReader = await CarReader.fromIterable(await randomCar(targetSize))
  const headers = process.env.CLUSTER_HEADERS ? JSON.parse(process.env.CLUSTER_HEADERS) : {}
  const cluster = new Cluster(process.env.CLUSTER_URL, { headers })

  const splitter = new TreewalkCarSplitter(carReader, 1024 * 1024 * 100)

  for await (const car of splitter.cars()) {
    // Each `car` is an AsyncIterable<Uint8Array>
    const carParts = []
    for await (const part of car) {
      carParts.push(part)
    }
    const carFile = new Blob(carParts, {
      type: 'application/car'
    })

    console.log('size', carFile.size)

    try {
      const res = await cluster.add(carFile)
      console.log(res)
    } catch (err) {
      console.log('err', err)
      console.error(err)
      if (err.response) console.error(await err.response.text())
    }
  }
}

main()

/**
 * @param {number} targetSize
 * @returns {Promise<AsyncIterable<Uint8Array>>}
 */
async function randomCar (targetSize) {
  const blocks = []
  let size = 0
  const seen = new Set()
  let it = 0
  while (size < targetSize) {
    const bytes = dagCbor.encode(
      // garbage(targetSize / 4, { weights: { CID: 0 } })
      garbage(randomInt(1, targetSize), { weights: { CID: 0 } })
    )
    const hash = await sha256.digest(bytes)
    const cid = CID.create(1, dagCbor.code, hash)
    if (seen.has(cid.toString())) continue
    seen.add(cid.toString())
    blocks.push({ cid, bytes })
    size += bytes.length
    it++
  }

  console.log('size final', size, it)
  const rootBytes = dagCbor.encode(blocks.map((b) => b.cid))
  console.log('roooot bytes', rootBytes.length)
  const rootHash = await sha256.digest(rootBytes)
  const rootCid = CID.create(1, dagCbor.code, rootHash)
  console.log('root cid', rootCid.byteLength)
  // @ts-ignore versions of multiformats...
  const { writer, out } = CarWriter.create([rootCid])
  // @ts-ignore versions of multiformats...
  writer.put({ cid: rootCid, bytes: rootBytes })
  console.log('blocks', blocks.length)
  // @ts-ignore versions of multiformats...
  blocks.forEach((b) => writer.put(b))
  writer.close()
  return out
}

/**
 * @param {number} min
 * @param {number} max
 */
function randomInt (min, max) {
  return Math.random() * (max - min) + min
}
