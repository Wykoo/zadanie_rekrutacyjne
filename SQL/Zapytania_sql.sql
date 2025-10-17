-- Mateusz Wykowski
-- 16.10.2025

--[Pt.1 Weryfikacja struktury danych]
-- W następującej części sprawdzam typy danych oraz kolejność kolumn w tabelach
-- Staram sie upewnić, aby dane miały poprawny format i są zgodne między tabelami
SELECT 
    column_name,
    data_type
FROM information_schema.columns
WHERE table_name ='test_baza1'
ORDER BY ordinal_position;

SELECT 
    column_name,
    data_type
FROM information_schema.columns
WHERE table_name ='test_baza2'
ORDER BY ordinal_position;

--[Pt.2 Eksploracja danych]
-- Patrzę na pierwsze 10 wierszy, aby sprawdzić zawartośc kolumn
-- W tabeli test_baza1 widzę, że pierwsze wiersze w kolumnie NUMBER są puste, więc pomijam je, aby zobaczyc rzeczywiste dane
select *
from public.test_baza1 tb1
WHERE NULLIF(TRIM("NUMBER"), '') IS NOT NULL
limit 10;

select *
from public.test_baza2 tb2
limit 10;

-- Zauważyłem, że ID reprezentuje klienta, a NUMBER jego usługę/kontrakt, 
-- dlatego dla pojedynczego ID może występować wiele numerów (relacja one-to-many (1:N))


--[Pt.3 Przygotowanie warstwy 'STAGING']
-- Kolumna NUMBER ma różne typy danych w dwóch tabelach: 
-- test_baza1 -> VARCHAR 
-- test_baza2 -> INT
-- co może powodować błędy przy łączeniu (JOIN).
--
-- Tworzę warstwę STAGING, w której:
-- * ujednolicam typ i format kolumny NUMBER do tekstu (TEXT),
-- * czyszczę ją z niecyfrowych znaków,
-- * konwertuje END_DT do typu timestamp,
-- * normalizuję wartości w SEGMENT do małych liter,
-- * nie usuwam pustych numerów, poniważ moga oznaczać klientów bez aktywnej usługi
--   zamieniam je natomiast na null, aby łatwiej było mi je lokalizowac w pózniejszej analizie

--
create or replace view public.stg_test_baza1 as 
	select
		"ID"::bigint										as id,			
		nullif(regexp_replace("NUMBER", '\D', '', 'g'), '') as number_txt,	--czyszczenie numeru z niepotrzebnych znaków 
		to_timestamp("END_DT", 'DD.MM.YYYY HH24::MI') 		as end_ts,		--konwertuje tekst z datą na timestamp
		DATE(to_timestamp("END_DT", 'DD.MM.YYYY HH24::MI')) as end_dt,		--rzutuje dane w formcaie 'DD-MM-YYYY HH:MI'
		lower(trim("SEGMENT"))								as segment
	from public.test_baza1;

create or replace view public.stg_test_baza2 as
	select
		"ID"::bigint												as id,
		nullif(regexp_replace("NUMBER"::text, '\D', '', 'g'), '')  	as number_txt,
		upper(trim("PLAN")) 										as plan	--Standaryzuje plan -> S, M, L, X
	from public.test_baza2;
	

-- Zadanie 1
-- Wybieram aktywne numery z segmentów 'small' i 'soho' (datę END_DT odpowiednio dostosowuje do warunku, gdzie END_DT jest w cioągu 90 dni od dziś)
-- i uzupełniam je rekomendacjami ofertowymi z drugiej tabeli, przypisująć domyslne wartości S/M w miejscach, gdzie nie występuje rekomendacja
create table public.temp AS
select
	stb1.number_txt,
	stb1.end_dt,
	stb1.segment,
	coalesce(
		stb2.plan,
		case
			when stb1.segment = 'soho' then 'S'
			when stb1.segment = 'small' then 'M'
		END 
	) as plan
from public.stg_test_baza1 stb1
left join public.stg_test_baza2 stb2
	using (id)
where
	stb1.end_dt > CURRENT_DATE
	and stb1.end_dt < CURRENT_DATE + interval '90 day'
	and stb1.segment IN ('small', 'soho');


-- Zadnie 2
-- Podsumowuje liczbę rekomendacji dla każdego planu i sortuję wynik wg. kolejności S < M < L < X
with clean_plan as(
	select
		COALESCE(NULLIF(plan, ''), 'brak informacji') AS plan,
		number_txt
	from public.temp
)
select 
	plan,
	COUNT(number_txt) as ilosc_rekomendacji
from clean_plan
group by
plan
order by 
	case
		plan 
		when 'S' then 1
		when 'M' then 2
		when 'L' then 3
		when 'X' then 4
		else 0
	end;


-- Zadanie 3
-- Dla każdego ID wybieram najczęściej występującą rekomendację planu.
-- W przypadku remisu wybieram plan o wyższym priorytecie wg. S < M < L < X
-- Puste lub NULL wartości plany są pomijane przy wyborze.
with most_frequent as (
	select
		id,
		plan,
		COUNT(*) as cnt,
		case plan
			when 'S' then 1
			when 'M' then 2
			when 'L' then 3
			when 'X' then 4
			else 0
		end as plan_rank
	from public.stg_test_baza2
	where 
		plan is not null and TRIM(plan) <> ''		--Nie biorę NULL/'' do rankingu
	group by 
	1, 2
),
--Dla każdego ID wybieram plan o największej liczbie wystapien
final_pick as(
		select 
		id,
		plan,
		cnt,
		row_number() over(
		partition by ID
		order by cnt desc, plan_rank desc --wybiera wyższą rekomendację, jeżeli dopasowana są tej samej liczność
		) as ranking
	from most_frequent
)
--Pokazuje koncowy wynik 
select 
id,
plan as rekomendacja
from final_pick
where ranking=1
order by id;


-- Zadanie 4
-- Dla każdego ID tworzę liste numerów pogrupowanych wg. planu S, M, L, X w formacie tekstowym
WITH agg AS (
  SELECT
    id,
    STRING_AGG(number_txt::text, ', ' ORDER BY number_txt)
      FILTER (WHERE plan = 'S' AND number_txt IS NOT NULL) AS s_nums,
    STRING_AGG(number_txt::text, ', ' ORDER BY number_txt)
      FILTER (WHERE plan = 'M' AND number_txt IS NOT NULL) AS m_nums,
    STRING_AGG(number_txt::text, ', ' ORDER BY number_txt)
      FILTER (WHERE plan = 'L' AND number_txt IS NOT NULL) AS l_nums,
    STRING_AGG(number_txt::text, ', ' ORDER BY number_txt)
      FILTER (WHERE plan = 'X' AND number_txt IS NOT NULL) AS x_nums
  FROM public.stg_test_baza2
  GROUP BY id
)
--Łączę listy numerów z poszczególnych plnanów w jeden tekst,
--Funkcją ARRAY_REMOVE usuwa puste elementy, natomiast funkcja ARRAY_TO_STRING skleja je w czytelny format
SELECT
  id,
  ARRAY_TO_STRING(
    ARRAY_REMOVE(ARRAY[
      CASE WHEN s_nums IS NOT NULL THEN FORMAT('S: %s', s_nums) END,
      CASE WHEN m_nums IS NOT NULL THEN FORMAT('M: %s', m_nums) END,
      CASE WHEN l_nums IS NOT NULL THEN FORMAT('L: %s', l_nums) END,
      CASE WHEN x_nums IS NOT NULL THEN FORMAT('X: %s', x_nums) END
    ], NULL),
    ', '
  ) AS lista
FROM agg
ORDER BY id;




	
