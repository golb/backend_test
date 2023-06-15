import faker from '@faker-js/faker';
import { MongoClient, Db, Collection } from 'mongodb';
import dotenv from 'dotenv';

dotenv.config();

interface Customer {
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

const generateCustomers = async (collection: Collection<Customer>) => {
  const customers: Customer[] = [];

  const batchSize = faker.datatype.number({ min: 1, max: 10 });

  for (let i = 0; i < batchSize; i++) {
    const customer: Customer = {
      firstName: faker.name.firstName(),
      lastName: faker.name.lastName(),
      email: faker.internet.email(),
      address: {
        line1: faker.address.streetAddress(),
        line2: faker.address.secondaryAddress(),
        postcode: faker.address.zipCode(),
        city: faker.address.city(),
        state: faker.address.stateAbbr(),
        country: faker.address.countryCode(),
      },
      createdAt: faker.date.past(),
    };

    customers.push(customer);
  }

  await collection.insertMany(customers);
};

const main = async () => {
  const uri = process.env.DB_URI;

  if (!uri) {
    console.error('DB_URI is not defined in the environment variables.');
    return;
  }

  const client = new MongoClient(uri);

  try {
    await client.connect();

    const db: Db = client.db();
    const collection: Collection<Customer> = db.collection('customers');

    while (true) {
      await generateCustomers(collection);
      await new Promise((resolve) => setTimeout(resolve, 200));
    }
  } finally {
    client.close();
  }
};

main().catch(console.error);
