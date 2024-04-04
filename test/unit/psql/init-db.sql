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

CREATE TABLE public.test4 (id integer, id2 date);
INSERT INTO public.test4 VALUES (10, '2022-04-01');
INSERT INTO public.test4 VALUES (20, '2022-05-01');
INSERT INTO public.test4 VALUES (30, '2022-06-01');
INSERT INTO public.test4 VALUES (40, '2022-07-01');
INSERT INTO public.test4 VALUES (50, '2022-08-01');

CREATE TABLE public.test5 (id integer, id2 date, id3 date);
INSERT INTO public.test5 VALUES (10, '2024-04-01', '2024-04-01');
INSERT INTO public.test5 VALUES (20, '2024-04-02', '2024-05-01');
INSERT INTO public.test5 VALUES (30, '2024-04-03', '2024-06-01');
INSERT INTO public.test5 VALUES (40, '2024-04-04', '2024-07-01');
INSERT INTO public.test5 VALUES (50, '2024-04-05', '2024-08-01');
INSERT INTO public.test5 VALUES (60, '2024-04-06', '2024-09-01');
INSERT INTO public.test5 VALUES (70, '2024-04-07', '2024-10-02');
INSERT INTO public.test5 VALUES (80, '2024-04-08', '2024-10-03');
INSERT INTO public.test5 VALUES (90, '2024-04-09', '2024-10-04');

CREATE TABLE public.test6 (id integer, id2 TIMESTAMP);
INSERT INTO public.test6 VALUES (10, '2024-04-01 10:00:00');
INSERT INTO public.test6 VALUES (20, '2024-04-01 11:00:00');
INSERT INTO public.test6 VALUES (30, '2024-04-01 12:00:00');
INSERT INTO public.test6 VALUES (50, '2024-04-01 13:00:00');
INSERT INTO public.test6 VALUES (40, '2024-04-01 14:00:00');

CREATE TABLE public.test7 (id bigint, id2 bigint, id3 bigint, id4 bigint);
INSERT INTO public.test7 VALUES (1000001, 1000001, 1000000001, 1000000001);
INSERT INTO public.test7 VALUES (2000001, 2      , 2000000001, 2);
INSERT INTO public.test7 VALUES (3000002, 3000002, 3000000002, 3000000002);

CREATE TABLE public.test8 (id integer, id2 float, id3 integer, id4 float, id5 float);
INSERT INTO public.test8 VALUES (10, 10.0, -10, -10.0, 10.0);
INSERT INTO public.test8 VALUES (20, 10.0, -20, -20.0, -20.0);
INSERT INTO public.test8 VALUES (30, 10.0, -30, -30.0, 30.0);
INSERT INTO public.test8 VALUES (40, 10.0, -40, -40.0, -40.0);
INSERT INTO public.test8 VALUES (50, 10.0, -50, -50.0, 50.0);

CREATE TABLE public.test9 (id integer);
INSERT INTO public.test9 VALUES (10);