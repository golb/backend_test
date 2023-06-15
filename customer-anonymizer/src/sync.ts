import { MongoClient, ChangeStream, Document } from 'mongodb';
import dotenv from 'dotenv';

dotenv.config();

interface Customer {
  _id: ObjectId;
  firstName: string;
  lastName: string;
  email: string;
  address: {
    line1: string;
    line2: string;
    postcode: string;
    city: string;
    state: string;
    country: string;
  };
  createdAt: Date;
}

function anonymizeCustomer(customer: Customer): Customer {
  const anonymizedCustomer: Customer = { ...customer };
  anonymizedCustomer.firstName = generateRandomString(8);
  anonymizedCustomer.lastName = generateRandomString(8);
  anonymizedCustomer.email = generateRandomString(8) + customer.email.substring(customer.email.indexOf('@'));
  anonymizedCustomer.address.line1 = generateRandomString(8);
  anonymizedCustomer.address.line2 = generateRandomString(8);
  anonymizedCustomer.postcode = generateRandomString(8);

  return anonymizedCustomer;
}

function generateRandomString(length: number): string {
  const characters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let randomString = '';

  for (let i = 0; i < length; i++) {
    const randomIndex = Math.floor(Math.random() * characters.length);
    randomString += characters[randomIndex];
  }

  return randomString;
}

async function insertAnonymizedDocuments(db: any, documents: Customer[]): Promise<void> {
  const collection = db.collection('customers_anonymised');
  await collection.insertMany(documents);
}

async function processDocuments(db: any, documents: Document[]): Promise<void> {
  const anonymizedDocuments = documents.map((document) => anonymizeCustomer(document));
  await insertAnonymizedDocuments(db, anonymizedDocuments as Customer[]);
}

async function startRealtimeSync(db: any): Promise<void> {
  const collection = db.collection('customers');

  const pipeline = [
    {
      $match: {
        $or: [
          { operationType: 'insert' },
          { operationType: 'update' }
        ]
      }
    }
  ];

  const changeStream: ChangeStream = collection.watch(pipeline);

  for await (const change of changeStream) {
    const documents = change.fullDocument ? [change.fullDocument] : [change.documentKey];
    await processDocuments(db, documents as Document[]);
  }
}

async function performFullSync(db: any): Promise<void> {
  const collection = db.collection('customers');
  const cursor = collection.find();

  const documents: Document[] = [];

  while (await cursor.hasNext()) {
    const document = await cursor.next();
    documents.push(document);

    if (documents.length === 1000) {
      await processDocuments(db, documents as Document[]);
      documents.length = 0;
    }
  }

  if (documents.length > 0) {
    await processDocuments(db, documents as Document[]);
  }

  process.exit(0);
}

async function connectAndSync(): Promise<void> {
  const dbUri = process.env.DB_URI;
  const dbName = 'your_database_name';

  if (!dbUri) {
    console.error('DB_URI environment variable is not set');
    process.exit(1);
  }

  const client = new MongoClient(dbUri, { useUnifiedTopology: true });

  try {
    await client.connect();
    const db = client.db(dbName);

    if (process.argv.includes('--full-reindex')) {
      await performFullSync(db);
    } else {
      await startRealtimeSync(db);
    }
  } catch (error) {
    console.error('An error occurred:', error);
  } finally {
    await client.close();
  }
}

connectAndSync();
