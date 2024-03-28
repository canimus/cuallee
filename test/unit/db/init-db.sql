
-- id1: all unique
-- id2: all unique
-- id3: all same
-- id4: all same
-- id5: 2 repeats
-- id6: 2 nulls
CREATE TABLE public.test1 (id integer, id2 integer, id3 integer, id4 integer, id5 integer, id6 integer);
INSERT INTO public.test1 VALUES (1, 6, 1, 2, 1, 1);
INSERT INTO public.test1 VALUES (2, 7, 1, 2, 1, NULL);
INSERT INTO public.test1 VALUES (3, 8, 1, 2, 2, 3);
INSERT INTO public.test1 VALUES (4, 9, 1, 2, 2, NULL);
INSERT INTO public.test1 VALUES (5, 10, 1, 2, 3, 5);


CREATE TABLE public.test2 (id integer, id2 integer, id3 integer, id4 integer);
INSERT INTO public.test2 VALUES (10, 100, 10.0, 10);
INSERT INTO public.test2 VALUES (20, 200, 20.0, 20);
INSERT INTO public.test2 VALUES (30, 300, 30.0, 30);
INSERT INTO public.test2 VALUES (40, 400, 40.0, 40);
INSERT INTO public.test2 VALUES (50, 500, 50.0, Null);

CREATE TABLE public.test3 (id integer, id2 integer, id3 integer);
INSERT INTO public.test3 VALUES (1, 10, 10.0);
INSERT INTO public.test3 VALUES (1, 10, 10.0);
INSERT INTO public.test3 VALUES (1, 10, 10.0);
INSERT INTO public.test3 VALUES (0, 10, 10.0);
INSERT INTO public.test3 VALUES (0, 10, 10.0);
INSERT INTO public.test3 VALUES (0, 50, 10.0);