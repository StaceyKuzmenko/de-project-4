Построение витрины, содержащей информацию о выплатах курьерам

**Описание задачи**

***Витрина содержит следующие поля:***

-	id — идентификатор записи.
-	courier_id — ID курьера, которому перечисляем.
-	courier_name — Ф. И. О. курьера.
-	settlement_year — год отчёта.
-	settlement_month — месяц отчёта, где 1 — январь и 12 — декабрь.
-	orders_count — количество заказов за период (месяц).
-	orders_total_sum — общая стоимость заказов.
-	rate_avg — средний рейтинг курьера по оценкам пользователей.
-	order_processing_fee — сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25.
-	courier_order_sum — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже).
-	courier_tips_sum — сумма, которую пользователи оставили курьеру в качестве чаевых.
-	courier_reward_sum — сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа).


Правила расчёта процента выплаты курьеру в зависимости от рейтинга, где r — это средний рейтинг курьера в расчётном месяце:
-	r < 4 — 5% от заказа, но не менее 100 р.;
-	4 <= r < 4.5 — 7% от заказа, но не менее 150 р.;
-	4.5 <= r < 4.9 — 8% от заказа, но не менее 175 р.;
-	4.9 <= r — 10% от заказа, но не менее 200 р.

Отчёт собирается по дате заказа. Если заказ был сделан ночью и даты заказа и доставки не совпадают, в отчёте стоит ориентироваться на дату заказа, а не дату доставки. Иногда заказы, сделанные ночью до 23:59, доставляют на следующий день: дата заказа и доставки не совпадёт. Это важно, потому что такие случаи могут выпадать в том числе и на последний день месяца. Тогда начисление курьеру относите к дате заказа, а не доставки.

***Описание слоя staging***

Для начала я хотела бы уточнить, что изначально у нас для решения задачи было дано 3 таблицы: restaurants, couriers, deliveries. Я изучила поля, которые нам необходимо отобразить в витрине и пришла к выводу, что нам не понадобится таблица restaurants, так как вся необходимая информация у нас есть в остальных двух. Поэтому я не загружала в субд restaurants.

Слой staging содержит таблицы couriers и deliveries (as is).

**DDL couriers:**

CREATE TABLE stg.couriers (

	id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT couriers_pkey PRIMARY KEY (id)

);

**DDL deliveries:**

CREATE TABLE stg.deliveries (

	id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT deliveries_pkey PRIMARY KEY (id)
);


***Описание слоя dds***

**DDL dm_couriers**

CREATE TABLE dds.dm_couriers (

	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	CONSTRAINT dm_couriers_courier_id_key UNIQUE (courier_id),
	CONSTRAINT dm_couriers_pkey PRIMARY KEY (id)
);

**DDL dm_deliveries**

CREATE TABLE dds.dm_deliveries (

	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	order_id int4 NOT NULL,
	order_ts timestamp NOT NULL,
	delivery_id varchar NOT NULL,
	courier_id int4 NOT NULL,
	address varchar NOT NULL,
	delivery_ts timestamp NOT NULL,
	rate int4 NOT NULL,
	sum numeric(19, 5) NOT NULL DEFAULT 0,
	tip_sum numeric(19, 5) NOT NULL DEFAULT 0,
	CONSTRAINT deliveries_pkey PRIMARY KEY (id),
	CONSTRAINT deliveries_rate_check CHECK ((rate > 0)),
	CONSTRAINT unique_delivery_id_key UNIQUE (delivery_id),
	CONSTRAINT dm_deliverier_couriers_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(courier_id),
 	CONSTRAINT dm_deliveries_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id)
);

***Описание слоя cdm***

**DDL dm_courier_ledger**


CREATE TABLE cdm.dm_courier_ledger (

	id serial4 NOT NULL,
	courier_id int4 NOT NULL,
	courier_name varchar NOT NULL,
	settlement_year int4 NOT NULL,
	settlement_month int4 NOT NULL,
	orders_count int4 NOT NULL,
	orders_total_sum numeric(19, 5) NOT NULL,
	rate_avg numeric(2, 1) NOT NULL,
	order_processing_fee numeric(19, 5) NOT NULL,
	courier_order_sum numeric(19, 5) NOT NULL,
	courier_tips_sum numeric(19, 5) NOT NULL,
	courier_reward_sum numeric(19, 5) NOT NULL,
	CONSTRAINT dm_courier_ledger_courier_order_sum_check CHECK ((courier_order_sum > (0)::numeric)),
	CONSTRAINT dm_courier_ledger_courier_reward_sum_check CHECK ((courier_reward_sum > (0)::numeric)),
	CONSTRAINT dm_courier_ledger_courier_tips_sum_check CHECK ((courier_tips_sum > (0)::numeric)),
	CONSTRAINT dm_courier_ledger_order_processing_fee_check CHECK ((order_processing_fee > (0)::numeric)),
	CONSTRAINT dm_courier_ledger_orders_count_check CHECK ((orders_count >= 0)),
	CONSTRAINT dm_courier_ledger_orders_total_sum_check CHECK ((orders_total_sum >= (0)::numeric)),
	CONSTRAINT dm_courier_ledger_pkey PRIMARY KEY (id),
	CONSTRAINT dm_courier_ledger_rate_avg_check CHECK ((rate_avg > (0)::numeric)),
	CONSTRAINT dm_courier_ledger_settlement_month_check CHECK (((settlement_month >= 1) AND (settlement_month <= 12))),
	CONSTRAINT dm_courier_ledger_settlement_year_check CHECK (((settlement_year >= 2020) AND (settlement_year <= 2099))),
	CONSTRAINT dm_courier_ledger_unique UNIQUE (courier_id, settlement_year, settlement_month),
	CONSTRAINT dm_courier_ledger_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id)
);

Для формирования итоговой витрины я использовала следующий запрос:

	truncate table cdm.dm_courier_ledger RESTART IDENTITY;

                    with c as (select 
                                    b.courier_id,
                                    b.order_total_sum,
                                    case when b.rate_avg < 4 and b.courier_initial_order_sum < 100 then 100
	                                    when b.rate_avg >= 4 and b.rate_avg < 4.5 and b.courier_initial_order_sum < 150 then 150
	                                    when b.rate_avg >= 4.5 and b.rate_avg < 4.9 and b.courier_initial_order_sum < 175 then 175
	                                    when b.rate_avg >= 4.9 and b.courier_initial_order_sum < 200 then 200
	                                else courier_initial_order_sum
	                                end courier_order_sum
	                            from (
                                    select 
                                        distinct dd.courier_id,
                                        sum(dd.sum) over (partition by dd.courier_id) order_total_sum,
                                        avg(dd.rate) over (partition by dd.courier_id) as rate_avg,
                                        case when avg(dd.rate) over (partition by dd.courier_id) < 4 then sum(dd.sum) over (partition by courier_id) * 0.05
	                                        when avg(dd.rate) over (partition by dd.courier_id) >= 4 and avg(dd.rate) over (partition by courier_id) < 4.5 then sum(dd.sum) over (partition by courier_id) * 0.07
	                                        when avg(dd.rate) over (partition by dd.courier_id) >= 4.5 and avg(dd.rate) over (partition by courier_id) < 4.9 then sum(dd.sum) over (partition by courier_id) * 0.08
	                                        when avg(dd.rate) over (partition by dd.courier_id) >= 4.9 then sum(dd.sum) over (partition by courier_id) * 0.1
                                        end courier_initial_order_sum
                                    from dds.dm_deliveries dd) as b)
                    insert into cdm.dm_courier_ledger (courier_id, 
							courier_name, 
							settlement_year, 
							settlement_month, 
							orders_count,
							orders_total_sum,
							rate_avg,
							order_processing_fee,
							courier_order_sum,
							courier_tips_sum,
							courier_reward_sum)    
                    select 
                        distinct dd.courier_id,
                        dc.courier_name,
                        extract (year from dd.order_ts) as settlement_year,
                        extract (month from dd.order_ts) as settlement_month,
                        count(dd.order_id) over (partition by dd.courier_id) as orders_count,
                        sum(dd.sum) over (partition by dd.courier_id) order_total_sum,
                        avg(dd.rate) over (partition by dd.courier_id) as rate_avg,
                        sum(dd.sum) over (partition by dd.courier_id) * 0.25 order_processing_fee,
                        c.courier_order_sum,
                        sum(dd.tip_sum) over (partition by dd.courier_id) as courier_tips_sum,
                        (c.courier_order_sum + sum(dd.tip_sum) over (partition by dd.courier_id)) * 0.95 as courier_reward_sum
                    from dds.dm_deliveries dd 
                    left join dds.dm_couriers dc on dd.courier_id = dc.id
                    left join c on dd.courier_id = c.courier_id
                    group by dd.courier_id, dd.order_ts, dc.courier_name, dd.order_id, dd.sum, dd.rate, dd.tip_sum, c.courier_order_sum;
