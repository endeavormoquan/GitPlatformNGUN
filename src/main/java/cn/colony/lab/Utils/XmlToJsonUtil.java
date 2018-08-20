package cn.colony.lab.Utils;

import java.util.List;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class XmlToJsonUtil {

	/**
	 * ��xmlת��ΪJSON����
	 * @param 
	 * @return fastjson.JSONObject
	 * @throws Exception
	 */
	public static JSONObject xmltoJsonObject(String xml) throws Exception {
	    JSONObject jsonObject = new JSONObject();
	    Document document = DocumentHelper.parseText(xml);
	    //��ȡ���ڵ�Ԫ�ض���  
	    Element root = document.getRootElement();
	    iterateNodes(root, jsonObject);
	    return jsonObject;  
	 }
	/**
	 * ����Ԫ��
	 * @param node Ԫ��
	 * @param json ��Ԫ�ر������֮��ŵ�JSON����
	 */
	@SuppressWarnings("unchecked")
	private static void iterateNodes(Element node,JSONObject json){ 
	    //��ȡ��ǰԪ�ص�����
	    String nodeName = node.getName();
	    //�ж��ѱ�����JSON���Ƿ��Ѿ����˸�Ԫ�ص�����
	    if(json.containsKey(nodeName)){
	        //��Ԫ����ͬ�����ж��
	        Object Object = json.get(nodeName);
	        JSONArray array = null;
	        if(Object instanceof JSONArray){
	            array = (JSONArray) Object;
	        }else {
	            array = new JSONArray();
	            array.add(Object);
	        }
	        //��ȡ��Ԫ����������Ԫ�� 
	        List<Element> listElement = node.elements();
	        if(listElement.isEmpty()){
	            //��Ԫ������Ԫ�أ���ȡԪ�ص�ֵ
	            String nodeValue = node.getTextTrim();
	            array.add(nodeValue);
	            json.put(nodeName, array);
	            return ;
	        }
	        //����Ԫ��
	        JSONObject newJson = new JSONObject();
	        //����������Ԫ��
	        for(Element e:listElement){
	            //�ݹ�  
	            iterateNodes(e,newJson);
	        }
	        array.add(newJson);
	        json.put(nodeName, array);
	        return ;
	    }
	    //��Ԫ��ͬ���µ�һ�α���
	    //��ȡ��Ԫ����������Ԫ��
	    List<Element> listElement = node.elements();
	    if(listElement.isEmpty()){
	        //��Ԫ������Ԫ�أ���ȡԪ�ص�ֵ
	        String nodeValue = node.getTextTrim();
	        json.put(nodeName, nodeValue);
	        return ;
	    }
	    //���ӽڵ㣬�½�һ��JSONObject���洢�ýڵ����ӽڵ��ֵ
	    JSONObject object = new JSONObject();
	    //��������һ���ӽڵ�  
	    for(Element e:listElement){
	        //�ݹ�  
	        iterateNodes(e,object);
	    }
	    json.put(nodeName, object);
	    return ;
	}
}
