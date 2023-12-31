from util.common import square_unit_transform, float_equal

class CostEstimator:
    '''
    造价结果类, 用于保存造价计算结果.
    
    计量项目的setter方法接受一个unit参数(默认"m^2"), 用于指定输入的value的单位, 可用的unit值包括:
    1. m^2: 平方米
    2. mm^2: 平方毫米
    3. cm^2: 平方厘米
    4. km^2: 平方千米
    5. in^2: 平方英寸
    6. ft^2: 平方英尺
    7. yd^2: 平方码
    
    估算项目: 
    1. Pile Foundation Unit Cost: 桩基础单位造价 300元/m^2 (总造价 = 300 * 地板面积之和)
    2. Preliminary Work Cost : 土建前期准备工作（场地平整、土方开挖、回填、地基处理）单位造价 225元/m^2 (总造价 = 225 * 场地投影面积)
    3. Reinforced Concrete Cost: 基础钢筋混凝土单位造价 600元/m^3 (总造价 = 600 * 地板面积之和 * 0.1)
    4. Roofing Works Cost: 屋面工程单位造价 320元/m^2 (总造价 = 320 * 屋面总面积)
    5. External Wall Cost: 外墙单位造价 250/m^2 (总造价 = 250 * 外墙总面积)
    6. Internal Wall Cost: 内墙单位造价 150/m^2 (总造价 = 150 * 内墙总面积)
    7. Flooring Cost: 地板单位造价 150/m^2 (总造价 = 150 * 地板总面积)
    8. HVAC System Cost: 暖通系统单位造价 550/m^2 (总造价 = 550 * 地板总面积)
    9. Electrical System Cost: 电气系统单位造价 250/m^2 (总造价 = 250 * 地板总面积)
    10. Water Supply and Drainage System Cost: 给排水系统单位造价 100/m^2 (总造价 = 100 * 地板总面积)
    11. Fire Fighting System Cost: 消防系统单位造价 200/m^2 (总造价 = 200 * 地板总面积)
    13. Weak Current System Cost: 弱电系统单位造价 100/m^2 (总造价 = 100 * 地板总面积)
    '''
    def __init__(self):
        self.model_name = ""
        
        self._metrics = {
            "total_site_area": 0, # 场地投影面积
            "total_slab_area": 0, # 地板面积之和
            "total_exterior_wall_area": 0, # 外墙总面积
            "total_interior_wall_area": 0, # 内墙总面积
            "total_roof_area": 0, # 屋面总面积
        }
        
        # 各个子项目单位造价
        self.pile_foundation_unit_cost = 300
        self.preliminary_work_unit_cost = 225
        self.reinforced_concrete_unit_cost = 600
        self.roofing_works_unit_cost = 320
        self.exterior_wall_unit_cost = 250
        self.interior_wall_unit_cost = 150
        self.flooring_unit_cost = 150
        self.hvac_system_unit_cost = 550
        self.electrical_system_unit_cost = 250
        self.water_supply_and_drainage_system_unit_cost = 100
        self.fire_fighting_system_unit_cost = 200
        self.weak_current_system_unit_cost = 100
        
        self._cost_items = {
            "total_pile_foundation_cost": 0, # 桩基础总造价
            "total_preliminary_work_cost": 0, # 土建前期准备工作总造价
            "total_reinforced_concrete_cost": 0, # 基础钢筋混凝土总造价
            "total_roofing_works_cost": 0, # 屋面工程总造价
            "total_exterior_wall_cost": 0, # 外墙总造价
            "total_interior_wall_cost": 0, # 内墙总造价,
            "total_flooring_cost": 0, # 地板总造价
            "total_hvac_system_cost": 0, # 暖通系统总造价,
            "total_electrical_system_cost": 0, # 电气系统
            "total_water_supply_and_drainage_system_cost": 0, # 给排水系统总造价
            "total_fire_fighting_system_cost": 0, # 消防系统总造价
            "total_weak_current_system_cost": 0, # 弱电系统总造价
        }
    
    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, CostEstimator):
            return False
        
        for key in self._cost_items.keys():
            if not float_equal(self._cost_items[key], __value._cost_items[key]):
                return False
        
        for key in self._metrics.keys():
            if not float_equal(self._metrics[key], __value._metrics[key]):
                return False
        return True
        
    def get_metric(self, metric_name):
        return self._metrics[metric_name]
    
    def set_site_area(self, area, unit="m^2"):
        self._metrics["total_site_area"] = square_unit_transform(area, unit)
        self._cost_items["total_preliminary_work_cost"] = self.preliminary_work_unit_cost * self._metrics["total_site_area"]
        
    def set_slab_area(self, area, unit="m^2"):
        self._metrics["total_slab_area"] = square_unit_transform(area, unit)
        self._cost_items["total_pile_foundation_cost"] = self.pile_foundation_unit_cost * self._metrics["total_slab_area"]
        self._cost_items["total_reinforced_concrete_cost"] = self.reinforced_concrete_unit_cost * self._metrics["total_slab_area"] * 0.1
        self._cost_items["total_flooring_cost"] = self.flooring_unit_cost * self._metrics["total_slab_area"]
        self._cost_items["total_hvac_system_cost"] = self.hvac_system_unit_cost * self._metrics["total_slab_area"]
        self._cost_items["total_electrical_system_cost"] = self.electrical_system_unit_cost * self._metrics["total_slab_area"]
        self._cost_items["total_water_supply_and_drainage_system_cost"] = self.water_supply_and_drainage_system_unit_cost * self._metrics["total_slab_area"]
        self._cost_items["total_fire_fighting_system_cost"] = self.fire_fighting_system_unit_cost * self._metrics["total_slab_area"]
        self._cost_items["total_weak_current_system_cost"] = self.weak_current_system_unit_cost * self._metrics["total_slab_area"]
        
    def set_exterior_wall_area(self, area, unit="m^2"):
        self._metrics["total_exterior_wall_area"] = square_unit_transform(area, unit)
        self._cost_items["total_exterior_wall_cost"] = self.exterior_wall_unit_cost * self._metrics["total_exterior_wall_area"]
        
    def set_interior_wall_area(self, area, unit="m^2"):
        self._metrics["total_interior_wall_area"] = square_unit_transform(area, unit)
        self._cost_items["total_interior_wall_cost"] = self.interior_wall_unit_cost * self._metrics["total_interior_wall_area"]
    
    def set_roof_area(self, area, unit="m^2"):
        self._metrics["total_roof_area"] = square_unit_transform(area, unit)
        self._cost_items["total_roofing_works_cost"] = self.roofing_works_unit_cost * self._metrics["total_roof_area"]

    def verify_job1(self):
        expected_results = {
            'total_pile_foundation_cost': 5032090.476845567, 
            'total_preliminary_work_cost': 916862.333438128, 
            'total_reinforced_concrete_cost': 1006418.0953691135, 
            'total_roofing_works_cost': 1007160.5355987903, 
            'total_exterior_wall_cost': 3019018.548113325, 
            'total_interior_wall_cost': 5463036.685912579, 
            'total_flooring_cost': 2516045.2384227836, 
            'total_hvac_system_cost': 9225499.207550207, 
            'total_electrical_system_cost': 4193408.730704639, 
            'total_water_supply_and_drainage_system_cost': 1677363.4922818558, 
            'total_fire_fighting_system_cost': 3354726.9845637116, 
            'total_weak_current_system_cost': 1677363.4922818558
        }
        pass

    def get_cost_items(self):
        # 返回cost_item所有字段的值
        return self._cost_items
    
    def __str__(self):
        res  = "{"
        project_total_cost = 0
        for items in self._cost_items.items():
            res += f'''({items[0]}: {items[1]}), \n'''
            project_total_cost += items[1]
        res += f"(Project total cost: {project_total_cost})}}"
        return res