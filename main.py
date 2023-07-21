import json

import pandas


def get_warehouse_tariff(orders_data):
    warehouse_tariff = {}

    for order in orders_data:
        warehouse_name = order['warehouse_name']

        if warehouse_name in warehouse_tariff:
            continue

        order_products = order['products']
        highway_cost = order['highway_cost']

        products_quantity = 0
        for product in order_products:
            products_quantity += product['quantity']

        cost_per_piece_product = highway_cost / products_quantity

        warehouse_tariff[warehouse_name] = cost_per_piece_product

    return warehouse_tariff


def get_all_products_data(orders_data, warehouse_tariff_list):
    all_products_data = []

    for order in orders_data:
        warehouse_name = order['warehouse_name']
        order_products = order['products']

        highway_warehouse_tariff = round(warehouse_tariff_list[warehouse_name], 1)

        for product in order_products:
            all_products_data.append({
                'order_id': order['order_id'],  # уникальный id заказа
                'warehouse_name': warehouse_name,  # склад откуда отправился заказ
                'highway_warehouse_tariff': highway_warehouse_tariff,  # стоимость доставки за единицу товара
                'product': product['product'],  # наименования продукта
                'price': product['price'],  # цена продажи за единицу товара
                'quantity': product['quantity']  # количество проданного товара
            })

    return all_products_data


def task_number_two(df_products):
    df_products['income'] = df_products['price'] * df_products['quantity']
    df_products['expenses'] = df_products['highway_warehouse_tariff'] * df_products['quantity']
    df_products['profit'] = df_products['income'] - df_products['expenses']

    df_total_product_data = df_products[
        ['product',
         'quantity',
         'income',
         'expenses',
         'profit']
    ].groupby(['product'], as_index=False).sum()

    return df_products


def task_number_three(df_products):
    df_total_order_profit = df_products[
        ['order_id',
         'profit']
    ].groupby(['order_id'], as_index=False).sum()

    orders_count = df_products['order_id'].count()
    total_profit = df_products['profit'].sum()

    average_profit = total_profit / orders_count


def task_number_four(df_products):
    df_warehouse_profit = df_products[[
        'warehouse_name',
        'profit'
    ]].groupby(['warehouse_name'], as_index=False).sum()

    df_product_profit_of_warehouse = df_products[[
        'warehouse_name',
        'product',
        'profit'
    ]].groupby(['warehouse_name', 'product'],
               as_index=False).sum()

    df_warehouse_data = df_product_profit_of_warehouse.merge(
        df_warehouse_profit,
        how='left',
        on='warehouse_name',
        suffixes=['_product', '_warehouse']
    )

    df_warehouse_data['percent_profit_product_of_warehouse'] =\
        df_warehouse_data['profit_product'] / df_warehouse_data['profit_warehouse'] * 100

    return df_warehouse_data


def task_number_five(df_warehouse_data):
    df_warehouse_data = df_warehouse_data.sort_values(by=[
        'warehouse_name',
        'percent_profit_product_of_warehouse'
    ], ascending=False)

    df_warehouse_data = df_warehouse_data.reset_index(drop=True)

    for i in range(len(df_warehouse_data)):
        if i == 0:
            df_warehouse_data['accumulated_percent_profit_product_of_warehouse'] = \
                df_warehouse_data.loc[i, 'percent_profit_product_of_warehouse']
            continue

        if df_warehouse_data.loc[i, 'warehouse_name'] != df_warehouse_data.loc[i - 1, 'warehouse_name']:
            df_warehouse_data.loc[i, 'accumulated_percent_profit_product_of_warehouse'] = \
                df_warehouse_data.loc[i, 'percent_profit_product_of_warehouse']
            continue

        df_warehouse_data.loc[i, 'accumulated_percent_profit_product_of_warehouse'] = \
            df_warehouse_data.loc[i, "percent_profit_product_of_warehouse"] + \
            df_warehouse_data.loc[i - 1, "accumulated_percent_profit_product_of_warehouse"]

    return df_warehouse_data


def task_number_six(df_warehouse_data):
    df_warehouse_data.loc[
        df_warehouse_data['accumulated_percent_profit_product_of_warehouse'] <= 70,
        'category'
    ] = 'A'

    df_warehouse_data.loc[(
            (df_warehouse_data['accumulated_percent_profit_product_of_warehouse'] > 70) &
            (df_warehouse_data['accumulated_percent_profit_product_of_warehouse'] <= 90)
    ), 'category'] = 'B'

    df_warehouse_data.loc[
        df_warehouse_data['accumulated_percent_profit_product_of_warehouse'] > 90,
        'category'
    ] = 'C'


def main():
    with open('json_files/trial_task.json', 'r', encoding='utf-8') as file:
        orders_data = json.loads(file.read())

    warehouse_tariff_list = get_warehouse_tariff(orders_data)  # Тарифы стоимости заказа со склада(Задание №1)

    all_products_data = get_all_products_data(orders_data, warehouse_tariff_list)

    df_products = pandas.DataFrame(all_products_data)

    df_products = task_number_two(df_products)  # Таблица по заданию №2

    task_number_three(df_products)  # Таблица и средняя прибыль по заданию №3

    df_warehouse_data = task_number_four(df_products)  # Таблица по заданию №4

    df_warehouse_data = task_number_five(df_warehouse_data)  # Таблица по заданию №5

    task_number_six(df_warehouse_data)  # Таблица по заданию №6


if __name__ == '__main__':
    main()
