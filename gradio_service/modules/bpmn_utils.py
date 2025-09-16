import xml.etree.ElementTree as ET
import graphviz

def visualize_bpmn(xml_path, output_name="bpmn_diagram"):
    # Загрузка XML
    tree = ET.parse(xml_path)
    root = tree.getroot()
    
    # Пространство имен BPMN
    ns = {"bpmn": "http://www.omg.org/spec/BPMN/20100524/MODEL"}
    
    # Создание графа
    dot = graphviz.Digraph()
    
    # Словарь для хранения элементов
    elements = {}

    # Парсинг элементов
    for element in root.findall(".//bpmn:*", ns):
        elem_id = element.get("id")
        elem_type = element.tag.split("}")[-1]
        
        # Добавление узлов
        if elem_type == "task":
            dot.node(elem_id, element.get("name", "Task"), shape="box", style="filled", fillcolor="#9ac0cd")
        elif elem_type == "startEvent":
            dot.node(elem_id, "Start", shape="circle", style="filled", fillcolor="#7ccd7c")
        elif elem_type == "endEvent":
            dot.node(elem_id, "End", shape="circle", style="filled", fillcolor="#cd5555")
        elif elem_type == "exclusiveGateway":
            dot.node(elem_id, "", shape="diamond", width="0.7", height="0.7", fillcolor="#eed5b7")

    # Парсинг связей
    for flow in root.findall(".//bpmn:sequenceFlow", ns):
        source = flow.get("sourceRef")
        target = flow.get("targetRef")
        dot.edge(source, target)

    # Сохранение и рендер
    dot.render(output_name, format="png", cleanup=True)
    print(f"Диаграмма сохранена как {output_name}.png")

if __name__ == "__main__":
    visualize_bpmn("test5.xml")
