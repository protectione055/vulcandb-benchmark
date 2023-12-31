from util.common import Timer
from task3_cost_estimation.task3 import CostEstimator

import ifcopenshell
import ifcopenshell.util.selector as selector
import ifcopenshell.util.element
        
def estimate_cost_workload1(ifc_file, name="20210219Architecture.ifc"):
    '''
    为模型20210219Architecture.ifc专门写的造价计算模型, 因为这个模型的楼板是通过IfcCovering表达的, 而不是通过IfcSlab.
    
    需要计算造价的实体类型有: 
    1. 场地: 第一层的IfcCovering的投影面积
    2. 地板: IfcCovering
    3. 外墙 : IfcWall / IfcWallStandardCase, 通过Pset_WallCommon中的IscExternal属性是否为True判断是否为外墙
    4. 内墙: IfcWall / IfcWallStandardCase, 通过Pset_WallCommon中的IscExternal属性是否为False判断是否为外墙
    5. 屋顶: IfcRoof
    '''
    cost_result = CostEstimator()
    cost_result.model_name = name

    # 1. 通过一楼所有的地板面积之和计算场地总面积
    total_site_area = 0
    storeys = ifc_file.by_type("IfcBuildingstorey")
    for storey in storeys:
        if storey.Name != "Level 1":
            continue
        elements = ifcopenshell.util.element.get_decomposition(storey)
        
        count_covering = 0
        for element in elements:
            if element.is_a("IfcCovering"):
                area = selector.get_element_value(element, "Dimensions.Area")
                total_site_area += area
                count_covering += 1
    cost_result.set_site_area(total_site_area, "ft^2")

    # 2. 计算地板面积之和
    total_slab_area = 0
    coverings = ifc_file.by_type("IfcCovering")
    # print("coverings: ", len(coverings))
    for covering in coverings:
        area = selector.get_element_value(covering, "Dimensions.Area")
        total_slab_area += area
    cost_result.set_slab_area(total_slab_area, "ft^2")
    
    # 3. 计算内/外墙总面积
    total_interior_wall_area = 0
    total_exterior_wall_area = 0
    walls = ifc_file.by_type("IfcWall")
    # print("walls: ", len(walls))
    for wall in walls:
        area = selector.get_element_value(wall, "Dimensions.Area")
        if selector.get_element_value(wall, "Pset_WallCommon.IsExternal"):
            total_exterior_wall_area += area
        else:
            total_interior_wall_area += area
    cost_result.set_exterior_wall_area(total_exterior_wall_area, "ft^2")
    cost_result.set_interior_wall_area(total_interior_wall_area, "ft^2")
    
    # 4. 计算屋面总面积
    total_roof_area = 0
    roofs = ifc_file.by_type("IfcRoof")
    # print("roofs: ", len(roofs))
    for roof in roofs:
        area = selector.get_element_value(roof, "Dimensions.Area")
        total_roof_area += area
    cost_result.set_roof_area(total_roof_area, "ft^2")
    
    return cost_result

class RawfileTask3Impl:
    def __init__(self):
        self.ifc_files = []
    
    @Timer.eclapse
    def prepare_data(self, workloads):
        for workload in workloads:
            ifc_file = ifcopenshell.open(workload)
            self.ifc_files.append(ifc_file)
        pass
    @Timer.eclapse
    def run(self):
        return estimate_cost_workload1(self.ifc_files[0])
    
    def cleanup(self):
        pass