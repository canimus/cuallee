
-- id1: all unique
-- id2: all same
-- id3: 2 repeats
-- id4: 2 nulls
CREATE TABLE public.test1 (id integer, id2 integer, id3 integer, id4 integer);
INSERT INTO public.test1 VALUES (1, 2, 1, 1);
INSERT INTO public.test1 VALUES (2, 2, 1, NULL);
INSERT INTO public.test1 VALUES (3, 2, 2, 3);
INSERT INTO public.test1 VALUES (4, 2, 2, NULL);
INSERT INTO public.test1 VALUES (5, 2, 3, 5);
