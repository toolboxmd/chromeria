import { defineConfig } from "@effect/graphql-generator/Config";

// `schema/github.graphql` is github.com's public schema
// (https://docs.github.com/public/fpt/schema.docs.graphql). Enterprise Server lacks some of its
// fields, such as pull request stacks, and validates a whole document before `@include` applies,
// so a field it lacks belongs in a document only github.com is sent.
export default defineConfig({
  schema: "./schema/github.graphql",
  documents: ["src/**/*.graphql"],
  scalars: {
    GitObjectID: "effect/Schema#String",
    GitRefname: "effect/Schema#String",
    // Preserve GitHub's strings for comparison with other reads.
    DateTime: "effect/Schema#String",
  },
});
