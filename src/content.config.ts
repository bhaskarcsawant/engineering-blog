import { defineCollection } from 'astro:content';
import { glob } from 'astro/loaders';
import { z } from 'astro/zod';

const blog = defineCollection({
	// Load Markdown and MDX files in the `src/content/blog/` directory.
	loader: glob({ base: './src/content/blog', pattern: '**/*.{md,mdx}' }),
	// Type-check frontmatter using a schema
	schema: ({ image }) =>
		z.object({
			title: z.string(),
			description: z.string(),
			// Transform string to Date object
			pubDate: z.coerce.date(),
			updatedDate: z.coerce.date().optional(),
			heroImage: z.optional(image()),
			previewHeroImage: z.optional(image()),
			// Each author field accepts either a single value or an array of values.
			// Use arrays for co-authored posts; the layout stacks avatars and joins names.
			authorName: z.union([z.string(), z.array(z.string())]).optional(),
			authorImage: z.union([image(), z.array(image())]).optional(),
			authorDesignation: z.union([z.string(), z.array(z.string())]).optional(),
			category: z.string().optional(),
			featured: z.boolean().optional(),
		}),
});

export const collections = { blog };
