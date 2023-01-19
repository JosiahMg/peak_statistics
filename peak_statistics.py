# -*- coding: utf-8 -*-
# Author: Mh
# Date: 2023/1/16 10:07
# Function: 统计每日计划量
import os
from datetime import timedelta

import numpy as np
import pandas as pd

FOLD_NAME = "peak_20230113"


class PeakStatistics:
    def __init__(self, fold_name):
        self.work_dir = os.path.join(fold_name, "data", "qd_high")
        self.req_batch_df = self.load_req_batch_no()
        self.gas_ids = ["79", "100100017", "100100020", "100100024", "100219112"]
        self.lng_ids = ['100100022', '10012167']

    def execute(self):
        self.calc_company_error_ratio()
        self.calc_gas_error_ratio()
        self.calc_lng_info()

    def calc_lng_info(self):
        df = pd.DataFrame([])
        for _, row in self.req_batch_df.iterrows():
            batch_no = row['req_batch_no']
            ts = row['ts'] + timedelta(days=-1)     # ts时刻的数据时间是昨天的因此-1d
            scada_df = self.load_scada_data(batch_no)
            lng_quantity = self.calc_lng_quantity(scada_df, ts)
            gas_user_quantity = self.calc_gas_user_quantity(scada_df, ts)
            merge_df = pd.merge(lng_quantity, gas_user_quantity, on='ts', how='inner')
            df = pd.concat([df, merge_df], axis=0)
        df.to_csv('lng_info.csv', index=False, encoding='gbk')

    def load_scada_data(self, batch_no):
        scada_path = os.path.join(self.work_dir, batch_no, 'scada')
        scada_name = ""
        filenames = os.listdir(scada_path)
        for filename in filenames:
            if filename.startswith("jzw"):
                scada_name = filename
                break
        scada_df = pd.read_csv(os.path.join(scada_path, scada_name))
        scada_df['gis_id'] = scada_df['gis_id'].astype(str)
        return scada_df

    def calc_gas_user_quantity(self, scada_df, ts):
        gas_df = scada_df[scada_df['gis_id'].isin(self.gas_ids)].copy()
        user_df = scada_df[scada_df['dno'] == 11].copy()
        gas_quantity = np.round(gas_df['flow_m3_h'].sum() / 10000, 2)
        user_quantity = np.round(user_df['flow_m3_h'].sum() / 10000, 2)
        diff_value = np.round((gas_quantity - user_quantity), 2)
        df = pd.DataFrame([{"ts": ts.strftime("%Y-%m-%d"), "总供气(万方)": gas_quantity,
                            "总用气(万方)": user_quantity, "差值(万方)": diff_value}])
        return df

    def calc_lng_quantity(self, scada_df, ts):
        lng_df = scada_df[scada_df['gis_id'].isin(self.lng_ids)].copy()
        lng_df = lng_df[['gis_id', 'flow_m3_h']]
        quantity_df = (lng_df.groupby('gis_id')['flow_m3_h'].sum()).rename('quantity').reset_index()
        lingang = quantity_df.loc[quantity_df['gis_id'] == '100100022', 'quantity'].values[0]
        tuanjielu = quantity_df.loc[quantity_df['gis_id'] == '10012167', 'quantity'].values[0]
        df = pd.DataFrame([{'ts': ts.strftime("%Y-%m-%d"), '临港LNG_m3': np.round(lingang, 2),
                            "团结路_m3": np.round(tuanjielu, 2)}])
        return df

    def calc_company_error_ratio(self):
        """ 计算城燃的误差率 """
        ratio_df = pd.DataFrame([])
        for _, row in self.req_batch_df.iterrows():
            print(f"process ts: {row['ts']}, batch number: {row['req_batch_no']}")
            next_ts = row['ts'] + timedelta(days=2)
            next_batch_no = self.transform_batch_no_by_ts(next_ts)
            if next_batch_no is None:
                print('company plan calc over')
                continue
            current_batch_no = row['req_batch_no']
            company_plan = self.load_company_plan(current_batch_no)
            real_company_plan = self.load_real_company_plan(next_batch_no)
            company_plan = company_plan[['company_name', 'plan_value_wm3']]
            merge_df = pd.merge(real_company_plan, company_plan, on='company_name')
            merge_df['ratio'] = np.round(
                (merge_df['plan_value_wm3'] - merge_df['quantity']) / merge_df['quantity'] * 100, 2)
            real_ts = row['ts'] + timedelta(days=1)  # ts时刻的批复量是时间是明天的 因此+1
            merge_df['ts'] = real_ts.strftime("%Y-%m-%d")
            ratio_df = pd.concat([ratio_df, merge_df], axis=0)
        ratio_df = ratio_df[['ts', 'company_name', 'plan_value_wm3', 'quantity', 'ratio']]
        ratio_df.rename(columns={"ts": "时间", "plan_value_wm3": "计划量", "quantity": "实际用量", "ratio": "误差率"}, inplace=True)
        ratio_df.to_csv('company_ratio.csv', index=False, encoding='gbk')

    def calc_gas_error_ratio(self):
        ratio_df = pd.DataFrame([])
        for _, row in self.req_batch_df.iterrows():
            current_batch_no = row['req_batch_no']
            gas_approv = self.load_gas_approval(current_batch_no)
            next_ts = row['ts'] + timedelta(days=2)  # ts时刻的批复量是时间是明天的 但是明日的数据在后日才采集到  因此+2
            next_batch_no = self.transform_batch_no_by_ts(next_ts)
            if next_batch_no is None:
                print('gas ratio calc over')
                continue
            real_gas = self.load_real_gas_quantity(next_batch_no)
            merge_df = pd.merge(real_gas, gas_approv, on='gis_id', how='inner')
            print(merge_df)
            merge_df = merge_df[['gis_id', 'quantity', 'plan_value_wm3', 'stationName']]
            merge_df['ratio'] = np.round(
                (merge_df['plan_value_wm3'] - merge_df['quantity']) / merge_df['quantity'] * 100, 2)
            real_ts = row['ts'] + timedelta(days=1)  # ts时刻的批复量是时间是明天的 因此+1
            merge_df['ts'] = real_ts.strftime("%Y-%m-%d")
            ratio_df = pd.concat([ratio_df, merge_df], axis=0)

        ratio_df = ratio_df[['ts', 'gis_id', 'stationName', 'plan_value_wm3', 'quantity', 'ratio']]
        ratio_df.rename(columns={"ts": "时间", "stationName": "门站", "plan_value_wm3": "计划量",
                                 "quantity": "实际用量", "ratio": "误差率"}, inplace=True)
        ratio_df.to_csv('gas_ratio.csv', index=False, encoding='gbk')

    def load_company_plan(self, batch_no):
        company_plan_path = os.path.join(self.work_dir, batch_no, "company_plan.csv")
        company_plan_df = pd.read_csv(company_plan_path)
        company_plan_df['gis_id'] = company_plan_df['gis_id'].astype(str)
        return company_plan_df

    def load_gas_approval(self, batch_no):
        gas_approv_path = os.path.join(self.work_dir, batch_no, "gas_approval.csv")
        gas_approval_df = pd.read_csv(gas_approv_path)
        # 泊里分输站
        POLI_GAS_CHILDREN = ["200287136", "100304635"]
        POLI_GAS_INFO = {"gis_id": "100219112", "name": "泊里分输站"}
        sub_df = gas_approval_df[gas_approval_df['gis_id'].isin(POLI_GAS_CHILDREN)]
        poli = pd.DataFrame([{"gis_id": POLI_GAS_INFO['gis_id'], "plan_value_wm3": sub_df['plan_value_wm3'].sum(),
                              "stationName": POLI_GAS_INFO['name'], "unit": sub_df['unit'].unique()[0]}])
        gas_approval_df = pd.concat([gas_approval_df, poli], axis=0, ignore_index=True)
        gas_approval_df = gas_approval_df[gas_approval_df['gis_id'].isin(self.gas_ids)].copy()
        return gas_approval_df

    def transform_batch_no_by_ts(self, ts):
        month = ts.month
        day = ts.day
        filter_df = self.req_batch_df[
            (self.req_batch_df['ts'].dt.day == day) & (self.req_batch_df['ts'].dt.month == month)]
        if filter_df.empty:
            return None
        batch_no = filter_df['req_batch_no'].values[0]
        return batch_no

    def load_req_batch_no(self):
        req_batch_name = os.path.join(self.work_dir, "req_batch_no.csv")
        req_batch_df = pd.read_csv(req_batch_name, encoding='utf-8')
        req_batch_df.sort_values('ts', inplace=True)
        req_batch_df['ts'] = pd.to_datetime(req_batch_df['ts'])
        req_batch_df['req_batch_no'] = req_batch_df['req_batch_no'].astype(str)
        req_batch_df['day'] = req_batch_df['ts'].dt.day
        req_batch_df.drop_duplicates('day', keep='last', inplace=True)
        return req_batch_df

    def load_company_info(self, batch_no):
        company_info_path = os.path.join(self.work_dir, batch_no, "company_info.csv")
        company_info_df = pd.read_csv(company_info_path)
        company_info_df = company_info_df[company_info_df['type'] == "工商户"].copy()
        return company_info_df

    def load_real_company_plan(self, next_batch_no):
        company_info = self.load_company_info(next_batch_no)
        company_info = company_info[['gis_id', 'company_name']]
        scada_path = os.path.join(self.work_dir, next_batch_no, 'scada')
        scada_name = ""
        filenames = os.listdir(scada_path)
        for filename in filenames:
            if filename.startswith("jzw"):
                scada_name = filename
                break
        scada_df = pd.read_csv(os.path.join(scada_path, scada_name))
        scada_df = scada_df[scada_df['dno'] == 11].copy()
        scada_df = scada_df[['gis_id', 'flow_m3_h']]
        merge_df = pd.merge(scada_df, company_info, on='gis_id', how='left')
        total_quantity = (merge_df.groupby('company_name')['flow_m3_h'].sum() / 10000).rename('quantity').reset_index()
        return total_quantity

    def load_real_gas_quantity(self, next_batch_no):
        scada_path = os.path.join(self.work_dir, next_batch_no, 'scada')
        scada_name = ""
        filenames = os.listdir(scada_path)
        for filename in filenames:
            if filename.startswith("jzw"):
                scada_name = filename
                break
        scada_df = pd.read_csv(os.path.join(scada_path, scada_name))
        scada_df = scada_df[scada_df['dno'] == 7].copy()
        scada_df['gis_id'] = scada_df['gis_id'].astype(str)
        scada_df = scada_df[['gis_id', 'flow_m3_h']]
        quantity_df = (scada_df.groupby('gis_id')['flow_m3_h'].sum() / 10000).rename('quantity').reset_index()
        quantity_df = quantity_df[quantity_df['gis_id'].isin(self.gas_ids)]
        return quantity_df


if __name__ == "__main__":
    PeakStatistics(FOLD_NAME).execute()
