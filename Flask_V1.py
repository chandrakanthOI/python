import json
from flask import Flask
from flask import jsonify
from flask import request
from flask import json
import traceback
from werkzeug.wsgi import ClosingIterator

class AS116:
    def __init__(self,process,organization,data):
        # INPUTS
        self.process=process
        self.org=organization
        self.data=data
        
        
        #SETUP PACKAGES
        from py2neo import Graph
        import networkx as nx
        from pymongo import MongoClient
        import regex as re
        self.nx=nx
        self.re=re
        self.Graph=Graph
        self.Db_N4j=Graph("bolt://18.223.39.218:7687", password= "0ne1ntegral")
        
        #SPECIAL IMPORTS
        SOT_info=self.Db_N4j.run("match (a:GROUP_COMPANY{name:{x}})<--(b:PROCESS{name:{y}}) return b.sot as SOURCE,b.process_code as CODE;",x=self.org,y=self.process).data()
        self.SOT=SOT_info[0]["SOURCE"]
        self.process_code=SOT_info[0]["CODE"]
        
        #MONGODB INTERFACE
        client=MongoClient("mongodb://OI:123456@18.223.39.218/mydb")
        self.DB=client.mydb
        self.index=self.DB["INDEX"]
        graph=self.index.find_one({"GRAPH":"0"})
        self.last_G_index=int(graph["GRAPH"][-1])
        self.Trans_detail=self.DB["DETAILS_TABLE"]
        self.Trans_table=self.DB["TRANSACTION_TABLE"]
        
    
    
    def BaseGraph(self,):
        "Creats Basegraph for the process passed "
        Subgraph=self.Db_N4j.run("match p=(a:GROUP_COMPANY{name:{x}})<-[]-(b:PROCESS{name:{y}})<-[*1..]-(c) with c match (c)-[r]->(d)  return c.name as Node, type(r) as REl,c.ooe as ORDer,c.derive as DErive,c.key as KEys,c.encode as ENcode,d.name as PArent,c.short_text as SHort;",x=self.org,y=self.process).to_data_frame()
        G=self.nx.Graph(name=self.process)# creating basegraph
        for i in range(len(Subgraph)):
            a=Subgraph["Node"][i]
            b=Subgraph["PArent"][i]
            r=Subgraph.REl[i]
            G.add_edge(a,b,assign=r)
            G.nodes[a]["ORDER"]=Subgraph["ORDer"][i]
            G.nodes[a]['DERIVE']=Subgraph["DErive"][i]   
            G.nodes[a]['name']=a
            G.nodes[a]["SHORT"]=Subgraph["SHort"][i]
            if Subgraph["KEys"][i]==None:
                pass
            else:
                G.nodes[a]["KEY"]=Subgraph["KEys"][i]
                G.nodes[a]["ENCODE"]=Subgraph["ENcode"][i]
        return G;
    
    def Preprocessing(self,Graph):
        self.data.fillna(method="ffill",inplace=True)
        from collections import OrderedDict
        "Formats the csv to be compatible with model"
        key1=self.FindNodes(key="KEY",value="1",graph=Graph)
        key2=self.FindNodes(key="KEY",value="2",graph=Graph)
        global m_string
        global o1_string
        global o2_string
        m_string=key1[0]
        o1_string=key2[0]
        o2_string=key2[1]
        
        
        # ENCODER_SUFFIXES
        global m_code
        global o1_code
        global o2_code
        m_code=Graph.nodes[m_string]["ENCODE"]
        o1_code=Graph.nodes[o1_string]["ENCODE"]
        o2_code=Graph.nodes[o2_string]["ENCODE"]
        
        #COLLECTING KEYS FROM CSV        
        master=list(OrderedDict.fromkeys(self.data[m_string][self.data.index[(self.data.SYSTEM==self.SOT)]]))
        Object1=list(OrderedDict.fromkeys(self.data[o1_string][self.data.index[(self.data.SYSTEM==self.SOT)]]))
        Object2=list(OrderedDict.fromkeys(self.data[o2_string][self.data.index[(self.data.SYSTEM==self.SOT)]]))
            
            
        #ENCODERS AND DECODERS
        M_ENC=self.index.find_one({"ENCODE":m_code})
        if M_ENC==None:
            M_ENC={"ENCODE":m_code,"ENCODER":{}}
        O1_ENC=self.index.find_one({"ENCODE":o1_code})
        if O1_ENC==None:
            O1_ENC= {"ENCODE":o1_code,"ENCODER":{}}
        O2_ENC=self.index.find_one({"ENCODE":o2_code})
        if O2_ENC==None:
            O2_ENC={"ENCODE":o2_code,"ENCODER":{}}
            
        
        #1.1 master encoder
        m_encoder=M_ENC["ENCODER"]
        global New_encoder
        New_encoder={}
        for i in (master):
            if i in m_encoder.keys():
                pass
            else:
                m_encoder[i]=m_code+str(len(m_encoder.keys())+1)
            New_encoder[i]=m_encoder[i]
                
        #2.1 OBJECT1 ENCODER
        o1_encoder=O1_ENC["ENCODER"]
        for j in (Object1):
            if j in o1_encoder.keys():
                pass
            else:
                o1_encoder[j]=o1_code+str(len(o1_encoder.keys())+1)
            New_encoder[j]=o1_encoder[j]
        
        #2.2 OBJECT2 ENCODER
        o2_encoder=O2_ENC["ENCODER"]
        for j in (Object2):
            if j in o2_encoder.keys():
                pass
            else:
                o2_encoder[j]=o2_code+str(len(o2_encoder.keys())+1)
            New_encoder[j]=o2_encoder[j]
        
        #REPLACING INTERNAL ID'S INTO THE CSV
        self.data.loc[:,m_string].replace(m_encoder,inplace=True)
        self.data.loc[:,o1_string].replace(o1_encoder,inplace=True)
        self.data.loc[:,o2_string].replace(o2_encoder,inplace=True)
        
        global decoder
        decoder={}
        for k,v in New_encoder.items():
            decoder[v]=k
        
        # UPDATING MONGO
        self.index.update_one({"ENCODE":m_code},{"$set":{"ENCODER":m_encoder}},True)
        self.index.update_one({"ENCODE":o1_code},{"$set":{"ENCODER":o1_encoder}},True) 
        self.index.update_one({"ENCODE":o2_code},{"$set":{"ENCODER":o2_encoder}},True)        
        return decoder,self.data;
    
    def GetExploder(self,):
        exploder_map=self.data[["SYSTEM",m_string,o1_string,o2_string]]
        exploder_map
        ## creating Exploder dictionary which is to be used as a map of explosion
        exploder={}
        master=list(exploder_map[m_string][exploder_map.index[(exploder_map.SYSTEM==self.SOT)]])
        for i in range(len(master)):    
            L1=list(exploder_map[o1_string][exploder_map.index[(exploder_map[m_string]==master[i])&(exploder_map["SYSTEM"]==self.SOT)]])
            L2=list(exploder_map[o2_string][exploder_map.index[(exploder_map[m_string]==master[i])&(exploder_map["SYSTEM"]==self.SOT)]])
            Object=L1+L2
            exploder[master[i]]=Object
        return exploder;
                                                    
    def expgraph(self,Tag,Graph,Head=None):
        "returns a collection of graphs relabeled for the Tag passed"
         # regex to find the pattern to replace in its name
        Regex=self.re.compile(r"^[A-Za-z]+") 
        mo=Regex.search(Tag)
        String=mo.group()
        # regex to find presence of its Head Key
        if Head==None:
            String1=None
        else:
            mo1=Regex.search(Head)
            String1=mo1.group()
        #3. identify node not to be relebeled which has #String tag in it.
        Regex2=self.re.compile(r"%s\d{1,5}"%String)
        # collection of nodes belonging to the process of operation
        Nodes=list(Graph.nodes)
        mapp={} # mapping dictionary for relabeling nodes
        for j in range(len(Nodes)):
            if Tag in Nodes[j] or Regex2.search(Nodes[j])!= None:
                # if the node has the relabelling tag already to it or if the node has related tag to the relabelling tag to it
                continue
            elif Head==None and String1 == None:\
                # if There is no Head parameter for the relabelling tag.
                item=Nodes[j].replace(String,Tag)
                mapp[Nodes[j]]=item
            elif Head not in Nodes[j] and String1 in Nodes[j]:
                # if the Head is not present in the node's name and String1 tag is present in nodes.
                continue
            else:
                item=Nodes[j].replace(String,Tag)
                mapp[Nodes[j]]=item
        Graph1=self.nx.relabel_nodes(Graph,mapp)
        graphs=Graph1# graph relabelled for item passed.
        return graphs; 

    def EXPLODEGRAPH(self,exploder,Graph,Head=None):
        Obj=self.re.compile(r"[A-Za-z]+")
        "Explodes the graph ->Graph in the format of dictionary ->exploder  exploder={master:[listofobjects]}"
        master=list(exploder.keys())
        graphs=[]
        for i in range(len(master)):
            grp_lvl1=self.expgraph(Tag=master[i],Graph=Graph,Head=Head)
            if type(exploder[master[i]])==list:
                Object=exploder[master[i]]
                graphs1=[]
                for j in range(len(Object)):
                    o1=Obj.search(Object[j])
                    o2=Obj.search(Object[j-1])
                    if o1.group()!=o2.group() and o2.group()!=None and j !=0: 
                        grp_lvl1=self.nx.compose_all(graphs1)
                    grp_lvl2=self.expgraph(Tag=Object[j],Graph=grp_lvl1,Head=master[i])
                    graphs1.append(grp_lvl2)
                Gu=self.nx.compose_all(graphs1)
                graphs.append(Gu)
            elif type(exploder[master[i]])==dict:
                graphs=self.EXPLODEGRAPH(exploder=exploder[master[i]],Graph=grp_lvl1,Head=master[i])
        if type(graphs)==list:
            GU=self.nx.compose_all(graphs)
        else:
            GU=graphs
        N=list(GU.nodes)
        for i in N:
            if o2_code+"_" in i:
                GU.remove_node(n=i)
                                                    
        return GU;
    
############################# Utility Functions#######################################################################################

    def FindNodes(self,key,value,graph):        
        "Identifies a node by its key and value and returns the node"
        N=list(graph.nodes)
        Node=[]
        for i in N:
            if key in graph.nodes[i].keys() and graph.nodes[i][key]==value:
                Node.append(i)
            else:
                pass        
        return Node;
    
    def indexmapper(self,String):
        "return column of search of index"
        global m_code
        global o1_code
        global o2_code
        index_map={m_code:m_string,o1_code:o1_string,o2_code:o2_string}
        regex=self.re.compile(r"[0-9]+")
        RE=self.re.compile(r"[A-Za-z]+\d{1,}")
        mo=regex.sub("",String)        
        if mo in index_map.keys():
            Value=index_map[mo]
        else:
            Mo=RE.search(String)
            mo1=regex.sub("",Mo.group())
            if mo1 in index_map.keys():
                Value=index_map[mo1] 
            else:
                Value=None
        return Value;
    
    def nodeExp(self,node):
        "finds the neo4j node name corresponding to the node passed"
        regex=self.re.compile(r"([A-Za-z]*)\d{1,}")
        mo=regex.sub(r"\1",node)
        return mo;
    
    def FetchValue(self,node,system):
        "Fetches Values from csv for a node in a system"
        if "name" not in GU.nodes[node].keys():
            return None
        Column=GU.nodes[node]["name"] #column of value
        Regex=self.re.compile(r"[A-Za-z]+\d{1,}")
        MO=Regex.findall(node) # collecting relative indexes of search
        Cond="(data[\"SYSTEM\"]==\"%s\")"%system
        for l in range(len(MO)):
            key=self.indexmapper(MO[l])
            val=MO[l]
            if key==None:
                pass
            else:
                S="&(data[\"%s\"]==\'%s\')"%(key,val)
                Cond=Cond+S
        try:
            Index=eval(Cond)# index of csv derived from relative indexes
            a=data.index[Index][0]
            Val=data[Column][a]
            if Val in decoder.keys():
                Value=decoder[Val]
            else:
                Value=Val            
            return float(Value);
        except ValueError: # returns the string values
            return str(Value)
        except KeyError:# checks if value present in default dictionary and return none if no column name is found in data            
            return None;
        except IndexError:# if a particular system value is not available in csv
            return None;
    
    def FindDerived(self,node,factor):
        import pandas as pd
        global GU
        Graph=GU
        import numpy as np
        import regex as re
        a=node
        exp=Graph.nodes[node]["DERIVE"]
        #Collecting Tags
        T=re.compile(r"[A-Za-z]+\d{1,}")
        Mo=T.findall(a)
        def COUNT_OCC(From,to,frequency,a,b):
            "returns the counts of cycles present between two dates"
            from datetime import datetime
            D1=FetchVal(node=From,Graph=Graph,Tag=Mo,factor="TRUTH")
            D2=FetchVal(node=to,Graph=Graph,Tag=Mo,factor="TRUTH")
            cycle=FetchVal(node=frequency,Graph=Graph,Tag=Mo,factor="TRUTH")
            Occ=[]
            if cycle.lower()=="monthly":
                factor=1
            elif cycle.lower()=="quarterly":
                factor=3
            elif cycle.lower()=="Half-yearly":
                factor=6
            else:
                factor=12             
            d1=datetime.strptime(D1,"%d/%m/%Y")
            d2=datetime.strptime(D2,"%d/%m/%Y")
            M=d2.date()-d1.date()
            Occ.append(M.days/(factor*30))
            return Occ[0];

        def DAYS(A,B):
            a=FindNode(node=A,key="LE")
            b=FindNode(node=B,key="LE")
            from datetime import datetime
            D1=datetime.strptime(Graph.nodes[a[0]]["TRUTH"],"%d/%m/%Y")
            D2=datetime.strptime(Graph.nodes[b[0]]["TRUTH"],"%d/%m/%Y")
            D=D1-D2
            return D.days;
        def SUM(a="STRING"):
            V=0
            L=list(Graph.nodes)
            for i in L:
                if "name"in Graph.nodes[i].keys() and Graph.nodes[i]["name"]==a:
                    V+=Graph.nodes[i]["TRUTH"]
                else:
                    pass
            return V;  
        def FindNode(node,sorter="Pymt",key=None):
            Nodes=list(Graph.nodes)
            N=[]
            for i in Nodes:
                if "name"in Graph.nodes[i] and Graph.nodes[i]["name"]==node:
                    if key==None:                    
                        N_reg=re.compile("%s(\d{1,})"%sorter)
                        mo=N_reg.search(i)
                        Num=int(mo.group(1))
                        N.insert(Num,i);
                    elif key in i:
                            N.append(i)
                    else:
                        pass
                else:
                    pass
            return N;


        def CREATE_PV(Payments="LE_Pymt_Amt",From="LE_Pymt_Effective_from",to="LE_Pymt_Effective_to"):
            from datetime import datetime
            Pmt=FindNode(Payments)
            Frm=FindNode(From)
            To=FindNode(to)
            Payments=[]
            D={}
            for i in range(len(Pmt)):
                a=datetime.strptime(Graph.nodes[Frm[i]]["TRUTH"],"%d/%m/%Y")
                b=datetime.strptime(Graph.nodes[To[i]]["TRUTH"],"%d/%m/%Y")
                A=a.date()
                B=b.date()
                D[Graph.nodes[Frm[i]]["TRUTH"]]=A
                D[Graph.nodes[To[i]]["TRUTH"]]=B
                Cycle=(A,B)
                Payments.append(Cycle)
            D1={}
            for k,v in D.items():
                D1[v]=k  
            def AssignPV(payment,incep_D_rate,trans_D_rate,rent,mode,cycle):
                from datetime import date
                from dateutil.relativedelta import relativedelta
                from dateutil import parser
                import pandas as pd
                from calendar import monthrange
                import numpy as np
                fromdate=payment[0]    
                enddate=payment[1]
                todate=fromdate+relativedelta(day=31)
                if todate<enddate:
                    # FROM DATE,TO DATE AND NO. OF DAYS
                    PV["LE_PV_Line_Calc_From"].append(str(fromdate))
                    PV["LE_PV_Line_Calc_To"].append(str(todate))
                    N_days=((todate-fromdate).days)+1
                    PV["LE_PV_Line_Calc_Days"].append(N_days)

                    # CUMULATIVE DAYS
                    Cum_days=sum(PV["LE_PV_Line_Calc_Days"])
                    PV["LE_PV_Line_Incep_Cum_Days"].append(Cum_days)
                    if fromdate<date(year=2019,month=4,day=1):
                        Cum1_days=None
                    else:
                        if PV["LE_PV_Line_Trans_Cum_Days"][-1]==None:
                            Cum1_days=N_days
                        else:
                            Cum1_days=N_days+PV["LE_PV_Line_Trans_Cum_Days"][-1]
                    PV["LE_PV_Line_Trans_Cum_Days"].append(Cum1_days)        
                    # RATE PER DAY        
                    Days_month=monthrange(month=fromdate.month,year=fromdate.year)
                    PV["LE_PV_Line_Incep_DiscP"].append(incep_D_rate)
                    PV["LE_PV_Line_Trans_DiscP"].append(trans_D_rate)
                    Pmt=(rent/Days_month[1])*N_days
                    # Payment minimum and PV calculation
                    PV["LE_PV_Line_MinPay"].append(Pmt)
                    pv1=np.pv(fv=-Pmt,nper=Cum_days,pmt=0,rate=incep_D_rate,when=cycle)
                    Asset_pv2=np.pv(fv=-Pmt,nper=Cum_days,pmt=0,rate=trans_D_rate,when=cycle)
                    if Cum1_days==None:
                        Liab_pv2=None
                        pv3=None
                    else:
                        Liab_pv2=np.pv(fv=-Pmt,nper=Cum1_days,pmt=0,rate=trans_D_rate,when=cycle) 
                        pv3=Liab_pv2
                    PV["LE_PV_Line_Mode"].append(mode)
                    PV["LE_PV_Line_Ret_Asset_Value"].append(pv1)
                    PV["LE_PV_Line_Ret_Liab_Value"].append(pv1)
                    PV["LE_PV_Line_MRet_Asset_Value"].append(Asset_pv2)
                    PV["LE_PV_Line_MRet_Liab_Value"].append(Liab_pv2)
                    PV["LE_PV_Line_MSim_Asset_Value"].append(pv3)
                    PV["LE_PV_Line_MSim_Liab_Value"].append(pv3)        
                    AssignPV([todate+relativedelta(days=1),enddate],incep_D_rate,trans_D_rate,rent,mode,cycle)
                else:
                    PV["LE_PV_Line_Calc_From"].append(str(fromdate))
                    PV["LE_PV_Line_Calc_To"].append(str(enddate))
                    N_days=((enddate-fromdate).days)+1
                    PV["LE_PV_Line_Calc_Days"].append(N_days)
                    Cum_days=sum(PV["LE_PV_Line_Calc_Days"])
                    PV["LE_PV_Line_Incep_Cum_Days"].append(Cum_days)
                    if fromdate<date(year=2019,month=4,day=1):
                        Cum1_days=None
                    else:
                        if PV["LE_PV_Line_Trans_Cum_Days"][-1]==None:
                            Cum1_days=N_days
                        else:
                            Cum1_days=N_days+PV["LE_PV_Line_Trans_Cum_Days"][-1]
                    PV["LE_PV_Line_Trans_Cum_Days"].append(Cum1_days)         
                    Days_month=monthrange(month=fromdate.month,year=fromdate.year)
                    PV["LE_PV_Line_Incep_DiscP"].append(incep_D_rate)
                    PV["LE_PV_Line_Trans_DiscP"].append(trans_D_rate)
                    Pmt=(rent/Days_month[1])*N_days
                    # Payment minimum and PV calculation
                    PV["LE_PV_Line_MinPay"].append(Pmt)
                    pv1=np.pv(fv=-Pmt,nper=Cum_days,pmt=0,rate=incep_D_rate,when=cycle)
                    Asset_pv2=np.pv(fv=-Pmt,nper=Cum_days,pmt=0,rate=trans_D_rate,when=cycle)
                    if Cum1_days==None:
                        Liab_pv2=None
                        pv3=None
                    else:
                        Liab_pv2=np.pv(fv=-Pmt,nper=Cum1_days,pmt=0,rate=trans_D_rate,when=1) 
                        pv3=Liab_pv2
                    PV["LE_PV_Line_Mode"].append(mode)
                    PV["LE_PV_Line_Ret_Asset_Value"].append(pv1)
                    PV["LE_PV_Line_Ret_Liab_Value"].append(pv1)
                    PV["LE_PV_Line_MRet_Asset_Value"].append(Asset_pv2)
                    PV["LE_PV_Line_MRet_Liab_Value"].append(Liab_pv2)
                    PV["LE_PV_Line_MSim_Asset_Value"].append(pv3)
                    PV["LE_PV_Line_MSim_Liab_Value"].append(pv3)        
                return PV;        
            PV={"LE_PV_Line_Calc_From":[],"LE_PV_Line_Calc_To":[],"LE_PV_Line_Calc_Days":[],"LE_PV_Line_Incep_Cum_Days":[],
                "LE_PV_Line_Trans_Cum_Days":[],"LE_PV_Line_Incep_DiscP":[],"LE_PV_Line_Trans_DiscP":[],
                "LE_PV_Line_MinPay":[],"LE_PV_Line_Mode":[],"LE_PV_Line_Ret_Asset_Value":[],"LE_PV_Line_Ret_Liab_Value":[],
                "LE_PV_Line_MRet_Asset_Value":[],"LE_PV_Line_MRet_Liab_Value":[],"LE_PV_Line_MSim_Asset_Value":[],"LE_PV_Line_MSim_Liab_Value":[]}
            for i in range(len(Pmt)):
                I_node=FindNode(node="LE_DiscRate_PerYear",key=New_encoder["Inception"])
                T_node=FindNode(node="LE_DiscRate_PerYear",key=New_encoder["Transition"])    
                INCEPT_RATE=Graph.nodes[I_node[0]]["TRUTH"]
                if len(T_node)==0:
                    TRANS_RATE=Graph.nodes[I_node[0]]["TRUTH"]
                else:
                    TRANS_RATE=Graph.nodes[T_node[0]]["TRUTH"]
                RENT=Graph.nodes[Pmt[i]]["TRUTH"]
                if RENT==0:
                    continue
                rate1=((1+(INCEPT_RATE/100))**(1/365.25)-1)
                if TRANS_RATE==None:
                    rate2=rate1
                else:
                    rate2=((1+(TRANS_RATE/100))**(1/365.25)-1)
                C_node=FindNode(node="LE_Pymt_Due_At",key=New_encoder[str(int(Graph.nodes[Pmt[i]]["TRUTH"]))])
                CYC=Graph.nodes[C_node[0]]["TRUTH"]
                if CYC=="Beginning":
                    cyc=1
                else:
                    cyc=0
                AssignPV(payment=Payments[i],incep_D_rate=rate1,trans_D_rate=rate2,rent=RENT,mode="Future Value",cycle=cyc)
            global PV_frame
            PV_frame=pd.DataFrame(PV)
            Exp_nodes=self.Db_N4j.run("match p=(a:LE_PV_Compute_Holder)<--(b) return collect(b.name) as NODE;").data()
            E=Exp_nodes[0]["NODE"]
            for i in range(len(PV_frame)):
                a="LE_PV_Compute_Holder".replace("LE",list(EXP.keys())[0])
                for j in range(len(E)):
                    if i ==0:
                        node=E[j]
                    else:
                        node=E[j].replace("Line","Line"+str(i))
                    Graph.add_edge(a,node,assign="process")
                    a=node
                    if E[j] in PV_frame.columns:
                        if isinstance(PV_frame[[E[j]]][i],np.int64)==True:
                            ITEM=int(PV_frame[E[j]][i])
                        elif isinstance(PV_frame[[E[j]]][i],np.object)==True:
                            ITEM=str(PV_frame[E[j]][i])
                        elif isinstance(PV_frame[E[j]][i],np.float64)==True:
                            ITEM=float(PV_frame[E[j]][i])
                        else :
                            ITEM=str(PV_frame[E[j]][i])                        
                        Graph.nodes[node]["TRUTH"]=ITEM
                            
                    else:
                        pass
            Graph.remove_nodes_from(E)
            Undisc=sum(PV_frame["LE_PV_Line_MinPay"])
            Undisc_node=FindNode(node="LE_Asset_UnDisc_Val",key="LE")
            Graph.nodes[Undisc_node[0]]["TRUTH"]=Undisc
            Appr_node=FindNode(node="LE_Approach",key="LE")
            approach=Graph.nodes[Appr_node[0]]["TRUTH"]
            if approach =="Retrospective":
                disc=sum(PV_frame["LE_PV_Line_Ret_Liab_Value"])
                Asset_val=sum(PV_frame["LE_PV_Line_Ret_Asset_Value"])
            elif approach=="Mod.Retrospective":
                DS=list(PV_frame["LE_PV_Line_MRet_Liab_Value"])
                AS=list(PV_frame["LE_PV_Line_MRet_Asset_Value"])
                disc=sum([x for x in DS if pd.isnull(x)==False])
                Asset_val=sum([x for x in AS if pd.isnull(x)==False])
            else:
                DS=list(PV_frame["LE_PV_Line_MSim_Liab_Value"])
                AS=list(PV_frame["LE_PV_Line_MSim_Asset_Value"])
                disc=sum([x for x in DS if pd.isnull(x)==False])
                Asset_val=sum([x for x in AS if pd.isnull(x)==False])
            print("ASSET,DISC",Asset_val,disc)
            Asset_node=FindNode(node="LE_Asset_Total_Cap_Value",key="LE")
            Liab_node=FindNode(node="LE_Asset_Total_Liab_Value",key="LE")
            Dep_node=FindNode(node="LE_Asset_Dep_RateP",key="LE")
            Disc_node=FindNode(node="LE_Asset_Disc_Val",key="LE")
            Tot_days=sum(PV_frame["LE_PV_Line_Calc_Days"])
            dep_rate=Asset_val/Tot_days
            Graph.nodes[Asset_node[0]]["TRUTH"]=int(Asset_val)
            Graph.nodes[Disc_node[0]]["TRUTH"]=int(disc)
            Graph.nodes[Liab_node[0]]["TRUTH"]=int(disc)
            Graph.nodes[Dep_node[0]]["TRUTH"]=int(dep_rate)
            return PV;        

        def CREATE_SCHEDULE(Payments="LE_Pymt_Amt",dep="LE_Asset_Dep_RateP",rate="LE_DiscRate_PerYear",From="LE_Pymt_Effective_from",to="LE_Pymt_Effective_to",approach="LE_Approach",Asset="LE_Asset_Total_Cap_Value",Liab="LE_Asset_Total_Liab_Value"):
            from dateutil import parser
            def AssignSchedule(payment,disc,asset,rate,dep_rate,rent,approach):
                global Trans_Asset_bal
                global Trans_Liab_bal
                global Trans_dep
                global Trans_acc_dep
                global Trans_int
                from datetime import date
                from dateutil.relativedelta import relativedelta 
                from calendar import monthrange
                import numpy as np
                Trans_date=date(year=2019,month=4,day=1)
                fromdate=payment[0]    
                enddate=payment[1]
                todate=fromdate+relativedelta(day=31)
                if todate<enddate:
                    # FROM DATE,TO DATE AND NO. OF DAYS
                    Start=fromdate
                    End=todate
                else:
                    Start=fromdate
                    End=enddate
                Schedule["LE_Sch_Line_Calc_From"].append(str(Start))
                Schedule["LE_Sch_Line_Calc_To"].append(str(End))
                N_days=((todate-fromdate).days)+1
                Schedule["LE_Sch_Line_Calc_Days"].append(int(N_days))
                Days_month=monthrange(month=fromdate.month,year=fromdate.year)
                Schedule["LE_Sch_Line_DiscP"].append(round(rate,7))
                #print(Schedule["L_Sch_Line_Liability_Op_Bal"],Schedule["L_Sch_Line_Liability_Cl_Bal"])
                if len(Schedule["LE_Sch_Line_Liability_Op_Bal"])==0:
                    if approach.lower()=="retrospective":
                        Op_bal=round(disc,4)
                    elif approach.lower()=="mod.retrospective" or approach.lower()=="mod.simplified":
                        if Start==Trans_date:
                            Op_bal=round(disc,4)
                        elif Start<Trans_date:
                            Op_bal=None
                else:
                    if approach.lower()=="retrospective":
                        Op_bal=round(Schedule["LE_Sch_Line_Liability_Cl_Bal"][-1],4)
                    elif approach.lower()=="mod.retrospective" or approach.lower()=="mod.simplified":
                        if Start==Trans_date:
                            Op_bal=round(disc,4)
                        elif Start<Trans_date:
                            Op_bal=None
                        else:
                            Op_bal=round(Schedule["LE_Sch_Line_Liability_Cl_Bal"][-1],4)                    
                Schedule["LE_Sch_Line_Liability_Op_Bal"].append(Op_bal)
                if Op_bal!=None:
                    Int_amt=round((Op_bal*(1+rate)**N_days)-Op_bal,7)
                else:
                    Int_amt=None                
                Schedule["LE_Sch_Line_Interest_Amt"].append(Int_amt)
                if approach.lower()=="retrospective":
                    Pmt=(rent/Days_month[1])*N_days
                elif approach.lower()=="mod.retrospective"or approach.lower()=="mod.simplified":
                    if Start<Trans_date:
                        Pmt=None
                    elif Start>=Trans_date:
                        Pmt=(rent/Days_month[1])*N_days                    
                Schedule["LE_Sch_Line_Repayment"].append(Pmt)
                if Op_bal!=None:
                    Cl_bal=round(Op_bal+Int_amt-Pmt,4)
                else:
                    Cl_bal=None
                Schedule["LE_Sch_Line_Liability_Cl_Bal"].append(Cl_bal)
                if len(Schedule["LE_Sch_Line_Asset_Op_Bal"])==0:
                    if approach.lower()=="retrospective" or approach.lower()=="mod.retrospective": 
                        A_Op_bal=round(asset,4)
                    elif approach.lower()=="mod.simplified":
                        if Start==Trans_date:
                            A_Op_bal=round(asset,4)
                        else:
                            A_Op_bal= None
                else:
                    if approach.lower()=="retrospective" or approach.lower()=="mod.retrospective":
                        A_Op_bal=round(Schedule["LE_Sch_Line_Asset_Cl_Bal"][-1],4)
                    elif approach.lower()=="mod.simplified":
                        if Start>Trans_date:
                            A_Op_bal=round(Schedule["LE_Sch_Line_Asset_Cl_Bal"][-1],4)
                        elif Start==Trans_date:
                            A_Op_bal=round(asset,4)
                        else:
                            A_Op_bal=None
                Schedule["LE_Sch_Line_Asset_Op_Bal"].append(A_Op_bal)
                if approach.lower()=="mod.simplified" and Start<Trans_date:
                    Dep=None
                    SC_close=None
                else:
                    Dep=dep_rate*N_days
                    SC_cl_bal=A_Op_bal-Dep
                    SC_close=round(SC_cl_bal,4)
                Schedule["LE_Sch_Line_Depreciation"].append(Dep)
                Schedule["LE_Sch_Line_Asset_Cl_Bal"].append(SC_close)
                if Start==Trans_date:
                    Trans_Asset_bal=A_Op_bal
                    Trans_Liab_bal=Op_bal
                    Trans_dep=Dep
                    Trans_acc_dep=Dep
                    Trans_int=Int_amt
                if todate<enddate:
                    AssignSchedule([todate+relativedelta(days=1),enddate],disc,asset,rate,dep_rate,rent,approach)
                return Schedule;
            from datetime import datetime        
            Pmt=FindNode(Payments)
            Frm=FindNode(From)
            To=FindNode(to)
            Payments=[]
            D={}
            for i in range(len(Pmt)):
                a=datetime.strptime(Graph.nodes[Frm[i]]["TRUTH"],"%d/%m/%Y")
                b=datetime.strptime(Graph.nodes[To[i]]["TRUTH"],"%d/%m/%Y")
                A=a.date()
                B=b.date()
                D[Graph.nodes[Frm[i]]["TRUTH"]]=A
                D[Graph.nodes[To[i]]["TRUTH"]]=B
                Cycle=(A,B)
                Payments.append(Cycle)
            D1={}
            for k,v in D.items():
                D1[v]=k  
            Appr=FindNode(approach,key="LE")
            app=Graph.nodes[Appr[0]]["TRUTH"]
            Assett=FindNode(Asset,key="LE")
            Liabb=FindNode(Liab,key="LE")
            asset=Graph.nodes[Assett[0]]["TRUTH"]
            liab=Graph.nodes[Liabb[0]]["TRUTH"]
            print("APPROACH",app)

            ## RATE PER DAY
            if app.lower()=="retrospective":
                search="Inception"
            else:
                search="Transition"
            Id=New_encoder[search]
            R_node=FindNode(rate,key=Id)
            if len(R_node)==0:
                Id=New_encoder["Inception"]
                R_node=FindNode(rate,key=Id)
            rate_P=Graph.nodes[R_node[0]]["TRUTH"]
            Dep_node=FindNode(dep,key="LE")
            Dep_rate=Graph.nodes[Dep_node[0]]["TRUTH"]
            #print("APP:%s,asset:%s,Liab:%s,rate:%s,Dep:%s"%(app,asset,liab,rate_P,Dep_rate))
            Schedule={"LE_Sch_Line_Calc_From":[],"LE_Sch_Line_Calc_To":[],"LE_Sch_Line_Calc_Days":[],"LE_Sch_Line_DiscP":[],
              "LE_Sch_Line_Liability_Op_Bal":[],"LE_Sch_Line_Interest_Amt":[],"LE_Sch_Line_Repayment":[],
              "LE_Sch_Line_Liability_Cl_Bal":[],"LE_Sch_Line_Asset_Op_Bal":[],"LE_Sch_Line_Depreciation":[],"LE_Sch_Line_Asset_Cl_Bal":[]}

            for i in range(len(Pmt)):            
                RENT=Graph.nodes[Pmt[i]]["TRUTH"]
                if RENT==0:
                    continue
                D_rate=((1+(rate_P/100))**(1/365.25)-1)
                AssignSchedule(payment=Payments[i],rate=D_rate,rent=RENT,asset=asset,dep_rate=Dep_rate,disc=liab,approach=app)
            ## TRANS GL CODES
            AS_GL_node=FindNode("LE_Asset_GLCode",key="LE")
            LI_GL_node=FindNode("LE_Liab_GLCode",key="LE")
            Dep_GL_node=FindNode("LE_Sch_Depreciation_GLCode",key="LE")
            aDep_GL_node=FindNode("LE_Sch_AccumDepreciation_GLCode",key="LE")
            Int_GL_node=FindNode('LE_Sch_Interest_GLCode',key="LE")
            Ret_GL_node=FindNode("LE_Retained_Earning_GL_Code",key="LE")
            
            #ADDING RENT POSTED COLUMN
            Schedule["LE_Sch_Line_Rent_Posted"]=[None for i in range(len(Schedule["LE_Sch_Line_Calc_From"]))]
            
            #Assigning values for each GL            
            Graph.nodes[AS_GL_node[0]]["TRUTH"]=int(Trans_Asset_bal)
            Graph.node[Dep_GL_node[0]]["TRUTH"]=int(Trans_dep)
            Graph.node[aDep_GL_node[0]]["TRUTH"]=int(Trans_acc_dep)
            Graph.nodes[Int_GL_node[0]]["TRUTH"]=int(Trans_int)
            Graph.nodes[Ret_GL_node[0]]["TRUTH"]=int(Trans_Liab_bal-Trans_Asset_bal)
            Graph.nodes[LI_GL_node[0]]["TRUTH"]=int(Trans_Liab_bal)      
            SCH=pd.DataFrame(Schedule)       
            Exp1_nodes=self.Db_N4j.run("match p=(a:LE_Sch_Holder)<-[*1..]-(b) return collect(b.name) as NODE;").data()
            E1=Exp1_nodes[0]["NODE"]
            for i in range(len(SCH)):
                a="LE_Sch_Holder"
                Node=E1[0].replace("Line","Line"+str(i))
                for j in range(1,len(E1)):
                    if j==0:
                        node=E1[j]
                    else:
                        node=E1[j].replace("Line","Line"+str(i))
                    Graph.add_edge(Node,node,assign="process")
                    if E1[j] in SCH.columns:
                        if isinstance(SCH[E1[j]][i],np.int64)==True:
                            ITEM=int(SCH[E1[j]][i])
                        elif isinstance(SCH[E1[j]][i],np.float64)==True:
                            ITEM=float(SCH[E1[j]][i])
                        elif isinstance(SCH[E1[j]][i],np.object)==True:
                            ITEM=str(SCH[E1[j]][i])
                        else:
                            ITEM=str(SCH[E1[j]][i])                            
                        Graph.nodes[node]["TRUTH"]=ITEM
                    else:
                        pass
                Graph.add_edge(a,Node,assign="process")
            Graph.remove_nodes_from(E1)        
            ### CONV DATATYPE TO FIT NODEjs
            return Schedule;

        def GEN_ID(Tag="Pymt"):
            import regex as re
            RE=re.compile(Tag+"\d{1,}")
            mo=RE.search(a)
            return mo.group()
        def FetchVal(node,Graph,Tag,factor):
            L=list(Graph.nodes)    
            for i in L:
                if  "name" in Graph.nodes[i].keys() and Graph.nodes[i]["name"]==node and all(tag in i for tag in Tag)==True:
                    val=Graph.nodes[i][factor]
                    return val
                else:
                    pass
            return None;
        FUNC=re.compile(r"DAYS\(.*\)|CREATE_PV\(.*\)|SUM\(.*\)|COUNT_OCC(.*)|CREATE_SCHEDULE(.*)|GEN_ID()")
        if FUNC.search(exp)!=None:
            v=eval(exp)
            return v;
        else:

            derive={".01":.01}
            string=exp.replace("%","*.01")
            regex=re.compile(r"\(.*\)") # pattern to isolate from string as varibles of operation         
            mo=regex.search(string)
            exp_v=mo.group()# variables of derivation
            obj=re.compile(r"([A-Z][a-zA-Z0-9_$%.]+)")
            exp1=obj.sub(r"derive['\1']",exp_v)
            var=obj.findall(exp_v)
            for i in var:
                derive[i]=FetchVal(node=i,Graph=Graph,Tag=Mo,factor="TRUTH")
            v=eval(exp1)
            return v;
    def SORTNODES(self,L,FACTOR="ORDER"):
        graph=GU
        "arranges nodes into a list by their order of execution"
        order=[]#list of the order of execution indexes
        for i in range (len(L)):
            try:
                O=int(graph.nodes[L[i]][FACTOR]) # gets their order of execution from information stored in nodes
                order.append(O)
            except KeyError:
                pass
            except TypeError:
                pass
        ORDER=list(set(order))
        ORDER.sort()# arranging indexes in ascending order
        NODE_LIST=[] # list of nodes arranged by their order of execution
        for j in range (len(ORDER)):
            for k in range(len(L)):
                try:
                    if int(graph.nodes[L[k]][FACTOR])==ORDER[j]:
                        NODE_LIST.append(L[k])
                    else:
                        pass
                except KeyError:
                    pass 
                except TypeError:
                    pass
        return NODE_LIST;
    
    def AssignInputs(self,):
        global GU
        "Adding Inputs from csv to nodes"
        Nodes=list(GU.nodes)
        for i in Nodes:    
            TRUTH=self.FetchValue(node=i,system=self.SOT)
            if "TRUTH" in GU.nodes[i].keys() and TRUTH == None and GU.nodes[i]["TRUTH"]!=None:
                pass
            else:
                GU.nodes[i]["TRUTH"]=TRUTH
        return GU;
    
    def AssignDerived(self,):
        import pandas as pd
        global GU
        "Adding derived values to the nodes"
        Nodes=self.SORTNODES(L=list(GU.nodes))
        for i in Nodes:
            Val=self.FindDerived(factor="TRUTH",node=i)
            GU.nodes[i]["TRUTH"]=Val
        return GU;
    
    def HeaderForJS(self,):
        global GU
        "returns Header info from the Graph"
        SUB=self.Db_N4j.run("match (a:LE_Header)<-[]-(b) return collect(b.name) as NM;").data()
        L=SUB[0]["NM"]
        Header={}
        for i in (L):
            name=i.replace("LE_",list(exploder.keys())[0]+"_")
            try:
                item=GU.nodes[name]["TRUTH"]
                if type(item)==float:
                    Header[GU.nodes[name]["SHORT"]]=round(item,2)
                else:
                    Header[GU.nodes[name]["SHORT"]]=item
            except KeyError:
                pass
        return Header;
    
    def DiscForJS(self,):
        "Returns Disc Rate Table for JS"
        D=["DiscRate1","DiscRate2"]
        L=list(GU.nodes)
        Disc={}
        for j in D:
            Disc[decoder[j]]={}
            for i in L:
                if j in i and "TRUTH" in GU.nodes[i].keys():
                    item=GU.nodes[i]["TRUTH"]
                    if type(item)==float:
                        Disc[decoder[j]][GU.nodes[i]["SHORT"]]=round(item,2)
                    else:
                        Disc[decoder[j]][GU.nodes[i]["SHORT"]]=item
        return Disc;
    
    def AssetForJS(self,):
        Asset={}
        L=list(GU.nodes)
        for i in L:
            if "Asset" in i and "TRUTH" in GU.nodes[i].keys() and "SHORT" in GU.nodes[i].keys():
                item=GU.nodes[i]["TRUTH"]
                if type(item)==float:
                    Asset[GU.nodes[i]["SHORT"]]=round(item,2)
                else:
                    Asset[GU.nodes[i]["SHORT"]]=item
                
        return Asset;
    
    def SummaryForJS(self,):
        import pandas as pd
        from datetime import date
        from datetime import timedelta
        "Returns Summary for js from PV table"
        v1=PV_frame
        Summary={"Retrospective":[date(month=4,day=1,year=2018),date(day=31,month=3,year=2019),date(day=31,month=3,year=2020)],
                 "Mod_Retrospective":[date(month=4,day=1,year=2019),date(month=3,day=31,year=2020)],
                 "Mod_Simplified":[date(month=4,day=1,year=2019),date(month=3,day=31,year=2020)]}
        for l in Summary.keys():
            if l=="Retrospective":
                Ass_ID="LE_PV_Line_Ret_Asset_Value"
                Liab_ID="LE_PV_Line_Ret_Liab_Value"
            elif l=="Mod_Retrospective":
                Ass_ID="LE_PV_Line_MRet_Asset_Value"
                Liab_ID="LE_PV_Line_MRet_Liab_Value"
            elif l=="Mod_Simplified":
                Ass_ID="LE_PV_Line_MSim_Asset_Value"
                Liab_ID="LE_PV_Line_MSim_Liab_Value"
            ID="LE_PV_Line_Calc_To"
            Record={}
            for i in range(len(Summary[l])):
                Rec={}
                if i==0:
                    Index=v1.index[(v1[ID]==str(Summary[l][i]-timedelta(days=1)))]
                    L=Index
                else:
                    Index=v1.index[(v1[ID]==(str(Summary[l][i])))]
                    Y_Index=L
                    L=Index
                    #Interest_Cost=sum(list(v1.loc[L:Index,"LE_PV_Line_"]))
                if len(Index)==0:
                    Index=0
                    Asset=v1[Ass_ID][Index]
                    Liab=v1[Liab_ID][Index]
                    if pd.isnull(Liab)==True:
                        Asset=list(v1[Ass_ID][Index+1])[0]
                        Liab=list(v1[Liab_ID][Index+1])[0]
                else:
                    Asset=list(v1[Ass_ID][Index])[0]
                    Liab=list(v1[Liab_ID][Index])[0]
                    if pd.isnull(Liab)==True:
                        Asset=list(v1[Ass_ID][Index+1])[0]
                        Liab=list(v1[Liab_ID][Index+1])[0]
                Ret_Earn=Asset-Liab
                Rec["ROU_ASSET"]=round(Asset,2)
                Rec["LEASE_LIAB"]=round(Liab,2)
                Rec["RETAINED_EARNINGS"]=round(Ret_Earn,2)
                Record[str(Summary[l][i])]=Rec
            Summary[l]=Record
        return Summary;
    
    def ScheduleForJS(self,):
        "Converts Dataframe to JS format"
        from dateutil import parser
        import datetime
        Sch_node=self.FindNode(node="LE_Sch_Holder",Graph=GU,key="LE")
        table=pd.DataFrame(GU.nodes[Sch_node[0]]["TRUTH"])       
        import numpy as np
        RECORD=[]
        Col=list(table.columns)
        for i in range(len(table)):
            rec={}
            for l in Col:
                Val=table[l][i]
                if isinstance(Val,np.float64)==True:
                    if l=="LE_Sch_Line_DiscP":
                        rec[l]=round(Val,6)
                    else:
                        rec[l]=round(Val,2)
                elif isinstance(Val,np.int64)==True:
                    rec[l]=int(Val)
                elif isinstance(Val,datetime.date):
                    rec[l]=parser.parse(str(Val))
                else:
                    rec[l]=str(Val)
            RECORD.append(rec)
        return RECORD;
    
    def PymtForJS(self,):
        "Returns Payment Information for JS"
        PYMT=[]
        k=list(decoder.keys())
        for i in k:
            if "Pymt" in i:
                PYMT.append(i)
        PY={}
        L=list(GU.nodes)
        for l in PYMT:
            Obj={}
            for i in L:
                if l in i:
                    if GU.nodes[i]["SHORT"] in Obj.keys() and Obj[GU.nodes[i]["SHORT"]]!=None:
                        pass
                    else:
                        item=GU.nodes[i]["TRUTH"]
                        if type(item)==float:
                             Obj[GU.nodes[i]["SHORT"]]=round(item,2)
                        else:
                             Obj[GU.nodes[i]["SHORT"]]=item                           
                       
            if len(Obj.keys())>0:
                PY[str(decoder[l])]=Obj
            else:
                pass
        return PY;
    
    def LessorForJS(self,):
        "Returns Lessor information  for JS"
        Lessor={}
        L=list(GU.nodes)
        for i in L:
            if "Lessor" in i and "TRUTH" in GU.nodes[i].keys():
                item=GU.nodes[i]["TRUTH"]
                if type(item)==float:
                    Lessor[GU.nodes[i]["SHORT"]]=round(item,2)
                else:
                    Lessor[GU.nodes[i]["SHORT"]]=item
                
        return Lessor;
    
#     def GetActual(self,external_id,posting_date,gl,data1,ID,DATE,GL,CR,DB):
#         v1=list(data1[CR][data1.index[(data1[ID]==external_id)&(data1[DATE]==posting_date)&(data1[GL]==gl)]])[0]
#         v2=list(data1[DB][data1.index[(data1[ID]==external_id)&(data1[DATE]==posting_date)&(data1[GL]==gl)]])[0]
#         if gl.lower()=="liab_gl" or gl.lower()=="accum_dep_gl":
#             if pd.isnull(v1)==True:
#                 Val=-v2
#             else:
#                 Val=v1
#         else:
#             if pd.isnull(v2)==True:
#                 Val=-v1
#             else:
#                 Val=v2
#         return Val;
    def AddRentPosted(self,graph,table,data,Id,column,gl):
        from collections import OrderedDict
        ID=self.FindTarget(key="SHORT",value="Lease ID",graph=graph)
        GL=self.FindTarget(key="SHORT",value="GL A/c",graph=graph)
        Dt=self.FindTarget(key="SHORT",value="Account Posting Dt",graph=graph)
        CR=self.FindTarget(key="SHORT",value="Credit",graph=graph)
        DB=self.FindTarget(key="SHORT",value="Debit",graph=graph)
        data[Dt]=pd.to_datetime(data[Dt])
        data[GL]=[x.lower() for x in data[GL]]
        post_dates=list(OrderedDict.fromkeys(data[Dt][data.index[(data[ID]==Id)]]))
        I=[]
        for i in post_dates: 
            try:
                v1=list(data[DB][data.index[(data[Dt]==i)&(data[ID]==Id)&(data[GL]==gl.lower())]])[0]
                v2=list(data[CR][data.index[(data[Dt]==i)&(data[ID]==Id)&(data[GL]==gl.lower())]])[0]
                if gl.lower()=="rent_gl":
                    if v1==None and v2!=None:
                        value=-v2
                    else:
                        value=v1
            except IndexError:
                value=None
            Index=list(table.index[(table.LE_Sch_Line_Calc_From<=i)&(table.LE_Sch_Line_Calc_To>=i)])[0]
            I.append(Index)
            v=table[column][Index]
            if v==None:
                table.at[Index,column]=value
            else:
                if Index in I:
                    table.at[Index,column]=v+value
                else:
                    table.at[Index,column]=value
        return table;
    
    def GroupPV(self,):
        import pandas as pd
        from dateutil import parser
        import datetime
        pv_node=self.FindNode(node="LE_PV_Compute_Holder",Graph=GU,key="LE")
        PV_frame=pd.DataFrame(GU.nodes[pv_node[0]]["TRUTH"])
        PV_Table={"Retrospective":["LE_PV_Line_Calc_From","LE_PV_Line_Calc_To","LE_PV_Line_Calc_Days","LE_PV_Line_Incep_Cum_Days",
                                   "LE_PV_Line_Incep_DiscP","LE_PV_Line_MinPay","LE_PV_Line_Mode",
                                   "LE_PV_Line_Ret_Asset_Value","LE_PV_Line_Ret_Liab_Value"],
                  "Mod_Retrospective":["LE_PV_Line_Calc_From","LE_PV_Line_Calc_To","LE_PV_Line_Calc_Days","LE_PV_Line_Incep_Cum_Days",
                                       "LE_PV_Line_Trans_Cum_Days","LE_PV_Line_Incep_DiscP","LE_PV_Line_Trans_DiscP","LE_PV_Line_MinPay",
                                       "LE_PV_Line_Mode","LE_PV_Line_MRet_Asset_Value","LE_PV_Line_MRet_Liab_Value"],
                  "Mod_Simplified":["LE_PV_Line_Calc_From","LE_PV_Line_Calc_To","LE_PV_Line_Calc_Days",
                                    "LE_PV_Line_Trans_Cum_Days","LE_PV_Line_Trans_DiscP",
                                    "LE_PV_Line_MinPay","LE_PV_Line_Mode","LE_PV_Line_MSim_Asset_Value","LE_PV_Line_MSim_Liab_Value"]}
        PV_rec={}
        for l in PV_Table.keys():
            Pv=[]
            for i in range(len(PV_frame)):
                L=PV_Table[l]
                Record={}
                for j in range(len(L)):
                    Val=PV_frame[L[j]][i]
                    if isinstance(Val,np.float64)==True:
                        Record[L[j]]=round(Val,6)
                    elif isinstance(Val,np.int64)==True:
                        Record[L[j]]=int(Val)
                    elif isinstance(Val,str):
                        Record[L[j]]=Val
                    elif isinstance(Val,datetime.date)==True:
                        Record[L[j]]=parser.parse(str(Val))
                Pv.append(Record)
            PV_rec[l]=Pv
        return PV_rec;


    def FindGLVal(self,gl,posting_date,table,process_code):
        from datetime import datetime
        if type(posting_date)==str:
            D1=datetime.strptime(posting_date,"%d/%m/%Y")
        else:
            D1=posting_date
        if process_code=="116C" and gl.lower()=="liab_gl":
            gl="interest_gl"
        elif process_code=="116E" and gl.lower()=="creditors_gl":
            gl="rent_gl"
        elif process_code=="116D" and gl.lower()=="liab_gl":
            gl="rent_gl"
        N={'retained_earning_gl':{"node":'LE_Retained_Earning_GL_Code',"search":['LE_Sch_Line_Asset_Op_Bal',"LE_Sch_Line_Liability_Op_Bal"]},
           'asset_gl':{"node":'LE_Asset_GLCode',"search":"LE_Sch_Line_Asset_Op_Bal"},
           'liab_gl': {"node":'LE_Liab_GLCode',"search":"LE_Sch_Line_Liability_Op_Bal"},
           "interest_gl":{"node":'LE_Sch_Interest_GLCode',"search":"LE_Sch_Line_Interest_Amt"},
           'accum_dep_gl':{"node":'LE_Sch_AccumDepreciation_GLCode',"search":'LE_Sch_Line_Depreciation'},
           'dep_gl':{"node":'LE_Sch_Depreciation_GLCode',"search":'LE_Sch_Line_Depreciation'},
           "rent_gl":{"node":"LE_Sch_Rent_GLCode","search":"LE_Sch_Line_Rent_Posted"}}
        Search=N[gl.lower()]["search"]
        Node=N[gl.lower()]["node"]
        try:
            ID=table.index[(table.LE_Sch_Line_Calc_From<=D1.date())&(table.LE_Sch_Line_Calc_To>=D1.date())]
        except AttributeError:
            ID=table.index[(table.LE_Sch_Line_Calc_From<=D1)&(table.LE_Sch_Line_Calc_To>=D1)]
        if type(Search)==list:
            S1=Search[1]
            S0=Search[0]
            V1=list(table[S1][ID])[0]
            V0=list(table[S0][ID])[0]
            Val=V1-V0
        else:
            Val=list(table[Search][ID])[0]
        line =list(ID)[0]
        return Node,Val,line;


        
    def GetDBandCR(self,gl,V_truth,V_sys,process_code):    
        Dict_cr={}
        Dict_db={} 
        if (gl.lower() =="liab_gl" or gl.lower() =="accum_dep_gl" or gl.lower()=="creditors_gl") and process_code!="116D":
            if V_truth==None:
                Dict_db["Audire"]=None
                Dict_cr["Audire"]=None            
            elif V_truth<0 :            
                Dict_db["Audire"]=abs(V_truth)
                Dict_cr["Audire"]=None
            else:
                Dict_cr["Audire"]=abs(V_truth)
                Dict_db["Audire"]=None
            if V_sys==None:
                Dict_db["ERP"]=None
                Dict_cr["ERP"]=None
            elif V_sys<0:
                Dict_db["ERP"]=abs(V_sys)
                Dict_cr["ERP"]=None
            else:
                Dict_db["ERP"]=None
                Dict_cr["ERP"]=abs(V_sys)
        elif gl.lower()=="rent_gl" and process_code=="116D":
            if V_truth==None:
                Dict_db["Audire"]=None
                Dict_cr["Audire"]=None   
            elif V_truth<0 :            
                Dict_db["Audire"]=abs(V_truth)
                Dict_cr["Audire"]=None
            else:
                Dict_cr["Audire"]=abs(V_truth)
                Dict_db["Audire"]=None
            if V_sys==None:
                Dict_db["ERP"]=None
                Dict_cr["ERP"]=None
            elif V_sys<0:
                Dict_db["ERP"]=abs(V_sys)
                Dict_cr["ERP"]=None
            else:
                Dict_db["ERP"]=None
                Dict_cr["ERP"]=abs(V_sys)            
        else:
            if V_truth==None:
                Dict_db["Audire"]=None
                Dict_cr["Audire"]=None   
            elif V_truth>=0 :            
                Dict_db["Audire"]=abs(V_truth)
                Dict_cr["Audire"]=None
            else:
                Dict_cr["Audire"]=abs(V_truth)
                Dict_db["Audire"]=None
            if V_sys==None:
                Dict_db["ERP"]=None
                Dict_cr["ERP"]=None            
            elif V_sys>=0:
                Dict_db["ERP"]=abs(V_sys)
                Dict_cr["ERP"]=None
            else:
                Dict_db["ERP"]=None
                Dict_cr["ERP"]=abs(V_sys)
        if Dict_db["Audire"]!=None and Dict_db["ERP"]==None:
            if Dict_cr["ERP"]!=None:
                Dict_db[gl.lower()+"_diff"]=Dict_db["Audire"]-Dict_cr["ERP"]
            else:
                Dict_db[gl.lower()+"_diff"]=Dict_db["Audire"]
            Dict_cr[gl.lower()+"_diff"]=None
            
        elif Dict_db["Audire"] !=None and Dict_db["ERP"]!=None:
            Dict_db[gl.lower()+"_diff"]=Dict_db["Audire"]-Dict_db["ERP"]
            Dict_cr[gl.lower()+"_diff"]=None
        
        elif Dict_cr["Audire"]!=None and Dict_cr["ERP"]==None:
            if Dict_db["ERP"]!=None:
                Dict_cr[gl.lower()+"_diff"]=Dict_cr["Audire"]-Dict_db["ERP"]
            else:
                Dict_cr[gl.lower()+"_diff"]=Dict_cr["Audire"]
            Dict_db[gl.lower()+"_diff"]=None
        elif Dict_cr["Audire"]!=None and Dict_cr["ERP"]!=None:
            Dict_cr[gl.lower()+"_diff"]=Dict_cr["Audire"]-Dict_cr["ERP"]
            Dict_db[gl.lower()+"_diff"]=None
        else:
            Dict_cr[gl.lower()+"_diff"]=None
            Dict_db[gl.lower()+"_diff"]=None            
        return Dict_db,Dict_cr;


    def FindTarget(self,key,value,graph):
        L=list(graph.nodes)
        for i in L:
            if key in graph.nodes[i].keys() and graph.nodes[i][key]==value:
                return i;
            else:
                pass
            
    def DefaultGL(self,process_code):
        if process_code=="116A":
            GL=["liab_gl","asset_gl","retained_earning_gl"]
        elif process_code=="116B":
            GL=["dep_gl","accum_dep_gl"]
        elif process_code=="116C":
            GL=["interest_gl","liab_gl"]
        elif process_code=="116E":
            GL=["rent_gl","creditors_gl"]
        elif process_code=="116D":
            GL=["rent_gl","liab_gl"]
        return GL;
    
    def FindNode(self,node,Graph,sorter="Pymt",key=None):
        "Finds the Node Based on its key"
        Nodes=list(Graph.nodes)
        N=[]
        for i in Nodes:
            if "name"in Graph.nodes[i] and Graph.nodes[i]["name"]==node:
                if key==None:                    
                    N_reg=re.compile("%s(\d{1,})"%sorter)
                    mo=N_reg.search(i)
                    Num=int(mo.group(1))
                    N.insert(Num,i);
                elif key in i:
                        N.append(i)
                else:
                    pass
            else:
                pass
        return N;

    def relativedate(self,date):
        from datetime import datetime
        from datetime import date as dt  
        from dateutil.relativedelta import relativedelta
        if type(date)==str:
            try:
                D1=datetime.strptime(date,"%d/%m/%Y").date()
            except ValueError:
                D1=datetime.strptime(date,"%Y-%m-%d").date()
        else:
            D1=date 
        Date=D1-relativedelta(day=1)
        Trans=dt(month=4,day=1,year=2019)
        try:
            if Date.date()<Trans:
                Date=Trans
        except AttributeError:
            if Date<Trans:
                Date=Trans
        return Date;

    def GetValFromTable(self,Id,date,gl,table,ID,Dt,GL,DB,CR,process_code):
        if ID not in list(table.columns):
            table[GL]=[x.lower() for x in list(table[GL])]
            Index=list(table.index[(table[Dt]==date)&(table[GL]==gl)])[0]
        else:
            Index=list(table.index[(table[ID]==Id)&(table[Dt]==date)&(table[GL]==gl.lower())])[0]       
        V1=table[DB][Index]
        V2=table[CR][Index]
        if type(V1)==dict and type(V2)==dict:
            if (gl.lower()=="liab_gl" or gl.lower()=="accum_dep_gl" or gl.lower()=="creditors_gl") and  process_code!="116D":
                if V2["Audire"]==None and V1["Audire"]!=None:
                    value=V1
                else:
                    value=V2
            elif gl.lower()=="rent_gl" and process_code=="116D":
                if V2["Audire"]==None and V1["Audire"]!=None:
                    value=V1
                else:
                    value=V2
            else:
                if V1["Audire"]==None and V2["Audire"]!=None:
                    value=V2
                else:
                    value=V1        
        else:
            if (gl.lower()=="liab_gl" or gl.lower()=="accum_dep_gl" or gl.lower()=="creditors_gl") and  process_code!="116D":
                if V2==None and V1!=None:
                    value=-V1
                else:
                    value=V2
            elif gl.lower()=="rent_gl" and process_code=="116D":
                if V2==None and V1!=None:
                    value=-V1
                else:
                    value=V2
            else:
                if V1==None and V2!=None:
                    value=-V2
                else:
                    value=V1        
        return value;

    def SubGraph(self,process_code):
        "Extracts Subgraph for the process passed"
        Subgraph=self.Db_N4j.run("match p=(a:PROCESS{process_code:{x}})<-[*1..]-(b) with b match (b)-[r]-(c) return b.name as NOde,type(r) as REl,c.name as PArent,b.short_text as SHort,b.data_type as DType;",x=process_code).to_data_frame()
        G=self.nx.Graph(name=process_code)# creating basegraph
        for i in range(len(Subgraph)):
            a=Subgraph["NOde"][i]
            b=Subgraph["PArent"][i]
            r=Subgraph.REl[i]
            G.add_edge(a,b,assign=r)  
            G.nodes[a]['name']=a
            G.nodes[a]["SHORT"]=Subgraph["SHort"][i]
        return G;
    
    def MiniProcess(self,Graph,process_code,data,Id,table,ACC=None):
    #################Functions explicit for Miniprocess###############################################################
        def PostingDates(date,table):
            from datetime import datetime
            if type(date)==str:
                D1=datetime.strptime(date,"%d/%m/%Y")
                ID=table.index[table.LE_Sch_Line_Calc_From>=D1.date()]
                posting_dates=list(table.LE_Sch_Line_Calc_From[ID])
            elif type(date)==list:
                posting_dates=[]
                for i in date:
                    if type(i)==str:
                        D1=datetime.strptime(i,"%d/%m/%Y")
                    else:
                        D1=i 
                    p_date=list(table.LE_Sch_Line_Calc_From[table.index[(table.LE_Sch_Line_Calc_From==D1.date())&(table.LE_Sch_Line_Calc_To>=D1.date())]])[0]
                    posting_dates.append(p_date)
            posting_dates=list(pd.to_datetime(posting_dates))
            return posting_dates;            

        import pandas as pd
        from collections import OrderedDict
        ID=self.FindTarget(key="SHORT",value="Lease ID",graph=Graph)
        GL=self.FindTarget(key="SHORT",value="GL A/c",graph=Graph)
        Dt=self.FindTarget(key="SHORT",value="Account Posting Dt",graph=Graph)
        CR=self.FindTarget(key="SHORT",value="Credit",graph=Graph)
        DB=self.FindTarget(key="SHORT",value="Debit",graph=Graph)
        Line_node=self.FindTarget(graph=Graph,key="SHORT",value="Ac Doc Line ID")
        Ref_node=self.FindTarget(graph=Graph,key="SHORT",value='Account Posting Ref')
        if isinstance(ACC,pd.DataFrame)==False:
            AC={Line_node:[],Dt:[],GL:[],DB:[],CR:[]}
            posting_dates=PostingDates(date="1/04/2019",table=table)
            if isinstance(data,pd.DataFrame)==False:
                pass
            else:
                data[Dt]=pd.to_datetime(data[Dt])
                data[GL]=[x.lower() for x in list(data[GL])]
            print(posting_dates)
            Line=0
            for p in posting_dates:
                Line+=1
                if isinstance(ACC,pd.DataFrame)==False:
                    Gl=self.DefaultGL(process_code)
                else:
                    Gl=list(set(data[GL][data.index[(data[ID]==Id)&(data[Dt]==p)]]))
                for i in Gl:
                    node,val,line=self.FindGLVal(gl=i,posting_date=p,table=table,process_code=process_code)
                    if isinstance(data,pd.DataFrame)==False:
                        val_sys=None
                    else:
                        val_sys=self.GetValFromTable(CR=CR,DB=DB,Dt=Dt,GL=GL,ID=ID,Id=Id,date=p,gl=i,table=data,process_code=process_code)
                    db,cr=self.GetDBandCR(V_sys=val_sys,V_truth=val,gl=i,process_code=process_code)
                    if i+"_diff" not in db.keys() or i+"_diff" not in cr.keys():
                        print(i,db,cr)
                    Node=self.FindNode(Graph=GU,key="LE",node=node)
                    GU.nodes[Node[0]]["DEBIT"]=db
                    GU.nodes[Node[0]]["CREDIT"]=cr
                    AC[GL].append(i)
                    AC[DB].append(db)
                    AC[CR].append(cr)
                    AC[Line_node].append("Line%d"%Line)
                    AC[Dt].append(p)
            AC_FRAME=pd.DataFrame(AC)
            AC_FRAME[Dt]=AC_FRAME[Dt].astype(str)
        else:
            data=data.astype({Dt:str})
            data[GL]=[x.lower() for x in list(data[GL])]
            ACC[GL]=[x.lower() for x in list(ACC[GL])]
            ACC[Dt]=pd.to_datetime(ACC[Dt])
            P_dates=list(OrderedDict.fromkeys(data[Dt][data.index[data[ID]==Id]]))
            Posting_dates=[]
            I=[]
            for i in P_dates:
                Posting_dates.append(self.relativedate(i))            
            for l in range(len(Posting_dates)):
                P_date=P_dates[l]
                Post_date=pd.Timestamp(Posting_dates[l])
                Gl=list(set(data[GL][data.index[(data[ID]==Id)&(data[Dt]==P_date)]]))
                for g in Gl:
                    Index=list(ACC.index[(ACC[Dt]==Post_date)&(ACC[GL]==g)])[0]                    
                    V_sys=self.GetValFromTable(CR=CR,DB=DB,Dt=Dt,GL=GL,ID=ID,Id=Id,date=P_date,gl=g,table=data,process_code=process_code)
                    Obj=self.GetValFromTable(CR=CR,DB=DB,Dt=Dt,GL=GL,ID=ID,Id=Id,date=Post_date,gl=g,table=ACC,process_code=process_code)
                    if isinstance(table,pd.DataFrame)==True:
                        p_date=pd.Timestamp(P_date)
                        n,v_audire,line=self.FindGLVal(gl=g,posting_date=p_date,table=table,process_code=process_code)
                        Obj["Audire"]=v_audire
                    print(Obj,V_sys,g)
                    V_Truth=Obj["Audire"]                    
                    if Index in I:
                        if Obj["ERP"]!=None:
                            Obj["ERP"]+=V_sys
                        else:
                            Obj["ERP"]=V_sys
                    else:
                        Obj["ERP"]=V_sys
                    I.append(Index)
                    V_Diff=V_Truth-Obj["ERP"]
                    Obj[g.lower()+"_diff"]=V_Diff
                    if (g.lower()=="liab_gl" or g.lower()=="accum_dep_gl" or g.lower()=="creditors_gl") and process_code!="116D":
                        ACC.at[Index,CR]=Obj
                    elif g.lower()=="rent_gl" and process_code=="116D":
                        ACC.at[Index,CR]=Obj
                    else:
                        ACC.at[Index,DB]=Obj
            AC_FRAME=ACC
            AC_FRAME[Dt]=AC_FRAME[Dt].astype(str)
        def AddSubProcess(AC_FRAME,Graph):
            L=list(AC_FRAME.columns)
            Line_node=self.FindTarget(graph=Graph,key="SHORT",value="Ac Doc Line ID")
            for l in L:
                if l not in list(Graph.nodes):
                    continue
                for i in range(len(AC_FRAME)): 
                    if i==0:
                        node=l
                    else:
                        node=l+"(%d)"%i
                    Graph.add_edge(Line_node,node)
                    if type(AC_FRAME[l][i])==dict:
                        Obj=AC_FRAME[l][i]
                        for k in Obj.keys():
                            Graph.nodes[node][k]=Obj[k]            
                    else:
                        Graph.nodes[node]["Audire"]=AC_FRAME[l][i]
            return Graph;
        def INFERENCE(Table,Graph,Dt,LN,GL,CR,DB,REF,process_code):
            from dateutil import parser
            from collections import OrderedDict
            import random
            REC=[]
            PD=list(OrderedDict.fromkeys(Table[Dt]))
            for i in PD:
                V=random.randint(25000000,35000000)
                Line=list(Table[LN][Table.index[(Table[Dt]==i)]])[0]
                Item={Graph.nodes[Dt]["SHORT"].upper().replace(" ","_"):parser.parse(str(i)),Graph.nodes[REF]["SHORT"].replace(" ","_").upper():V,Graph.nodes[LN]["SHORT"].upper().replace(" ","_"):Line}
                Gl=list(Table[GL][Table.index[Table[Dt]==i]])
                for j in Gl:
                    Item[j.lower()]={}        
                    Index=Table.index[(Table[Dt]==i)&(Table[GL]==j.lower())]
                    O1=list(Table[CR][Index])[0]
                    O2=list(Table[DB][Index])[0]
                    try:
                        if (j.lower()=="liab_gl" or j.lower()=="accum_dep_gl" or j.lower()=="creditors_gl") and process_code!="116D": 
                            diff=-int(O1[j.lower()+"_diff"])
                        elif j.lower()=="rent_gl" and process_code=="116D":
                            diff=-int(O1[j.lower()+"_diff"])
                        else:
                            diff=int(O2[j.lower()+"_diff"])
                    except ValueError:
                        diff=None
                    except TypeError:
                        diff=None
                    Item[j][Graph.nodes[CR]["SHORT"].upper().replace(" ","_")]=O1
                    Item[j][Graph.nodes[DB]["SHORT"].upper().replace(" ","_")]=O2
                    Item[j][j.lower()+"_diff"]=diff
                REC.append(Item)
            return REC;

        g3=AddSubProcess(AC_FRAME,Graph=Graph)
        INFERENCE=INFERENCE(Table=AC_FRAME,Graph=Graph,Dt=Dt,CR=CR,DB=DB,LN=Line_node,GL=GL,REF=Ref_node,process_code=process_code)    
        return AC_FRAME,INFERENCE;

def CODEAS116(JSON):
    import json
    import warnings
    warnings.filterwarnings("ignore")
    from networkx.readwrite import json_graph
    global pd
    global np
    import pandas as pd
    import numpy as np
    process=JSON["process"]
    transaction_id=JSON["transaction_id"]
    path="C:/Users/CHAN/Desktop/Audire_process/%s.csv"%process
    global data
    data=pd.read_csv(path)
    SubProcess={"INDAS116_Lease_Accounting_Inception_Transition":{"code":"116A","update":"INCEPTION/TRANSITION_TABLE"},
                "INDAS116_Lease_Accounting_Depreciation":{"code":"116B","update":"DEPRECIATION/ACCUMULATED_DEPRECIATION_TABLE"},
                "INDAS116_Lease_Accounting_Liabilities":{"code":"116C","update":"COST/LIABILITY_TABLE"},
                "INDAS116_Lease_Accounting_Liability_Reversals":{"code":"116D","update":"LIABILITY_REVERSALS_TABLE"},
                "INDAS116_Lease_Accounting_Rent_Posting":{"code":"116E","update":"RENT_POSTED_TABLE"},}   
    global GU  
    if process in SubProcess.keys():
        EXT="LE_%s_Ac_L_ExternalID"%SubProcess[process]["code"]
        DB="LE_%s_Ac_Dr"%SubProcess[process]["code"]
        CR="LE_%s_Ac_Cr"%SubProcess[process]["code"]
        try:
            data1=data.astype({EXT:str,DB:float,CR:float})
            Trans=AS116(data=data1,organization="XYZ Company",process=process)
        except ValueError:
            Trans.Trans_table.update_one({"TRANSACTION_ID":transaction_id,"STATUS":"In Progress"},{"$set":{"STATUS":"Failed"}})
            return {"Error":"DATA FORMAT MISMATCH"};        
        Details=Trans.Trans_detail
        global t
        t=Trans.Trans_table.find_one({"TRANSACTION_ID":transaction_id,"STATUS":"In Progress"})
        G1=Trans.SubGraph(process_code=SubProcess[process]["code"])
        Id_node=Trans.FindTarget(graph=G1,key="SHORT",value="Lease ID")
        from collections import OrderedDict
        Keys=list(OrderedDict.fromkeys(data1[Id_node]))
        processes=list(SubProcess.keys())+["Lease_Asset_Master_Setup"]
        l=0
        for i in Keys:
            v=Details.find_one({"EXTERNAL_ID":i,"STATUS":"Active"})
            if v==None:
                print(i)
                Details.insert_one({"TRANSACTION_ID":transaction_id,"EXTERNAL_ID":i,"STATUS":"Failed","PROCESS":processes,
                                    "SYSTEM":["AUDIRE","ERP"],"INFERENCE":"AUDIRE SETUP IS NOT FOUND FOR THE LEASE"})
            else:
                l+=1
                G1=json_graph.node_link_graph(v["GRAPHS"][SubProcess[process]["code"]])
                if process=="INDAS116_Lease_Accounting_Rent_Posting" or process=="INDAS116_Lease_Accounting_Liability_Reversals":
                    #APPENDING RENT POSTED
                    global table
                    GU=json_graph.node_link_graph(v["GRAPHS"]["SETUP"])
                    Sch_node=Trans.FindNode(node="LE_Sch_Holder",Graph=GU,key="LE")
                    if type(GU.nodes[Sch_node[0]]["TRUTH"])==dict:
                        table=pd.DataFrame(GU.nodes[Sch_node[0]]["TRUTH"])
                    else:
                        table=pd.read_json(GU.nodes[Sch_node[0]]["TRUTH"])
                    table.LE_Sch_Line_Calc_From=pd.to_datetime(table.LE_Sch_Line_Calc_From)
                    table.LE_Sch_Line_Calc_To=pd.to_datetime(table.LE_Sch_Line_Calc_To)
                    global STP
                    if "TRUTH" in G1.nodes[process].keys():
                        setup=G1.nodes[process]["TRUTH"]
                        STP=pd.read_json(setup)
                        Date_Column="LE_%s_Ac_Posting_Dt"%SubProcess[process]["code"]
                        STP[Date_Column]=pd.to_datetime(STP[Date_Column])
                    else:
                        STP=None
                    if process=="INDAS116_Lease_Accounting_Rent_Posting":
                        table=Trans.AddRentPosted(graph=G1,table=table,data=data1,Id=i,gl="rent_gl",column="LE_Sch_Line_Rent_Posted")
                    STP1,INF=Trans.MiniProcess(ACC=STP,Graph=G1,Id=i,data=data1,process_code=SubProcess[process]["code"],table=table)
                    table=table.astype({"LE_Sch_Line_Calc_From":str,"LE_Sch_Line_Calc_To":str})
                    GU.nodes[Sch_node[0]]['TRUTH']=table.to_json()
                    v["GRAPHS"]["SETUP"]=json_graph.node_link_data(GU)              
                    
                else:                    
                    setup=G1.nodes[process]["TRUTH"]
                    STP=pd.read_json(setup)
                    Date_Column="LE_%s_Ac_Posting_Dt"%SubProcess[process]["code"]
                    STP[Date_Column]=pd.to_datetime(STP[Date_Column])
                    STP1,INF=Trans.MiniProcess(ACC=STP,Graph=G1,Id=i,data=data1,process_code=SubProcess[process]["code"],table=None)
                G1.nodes[process]["TRUTH"]=STP1.to_json()
                v[SubProcess[process]["update"]]=INF
                v["TRANSACTION_ID"]=transaction_id
                v["GRAPHS"][SubProcess[process]["code"]]=json_graph.node_link_data(G1)
                Details.update_many({"EXTERNAL_ID":i},{"$set":{"STATUS":"Inactive"}})
                Details.insert_one({"TRANSACTION_ID":transaction_id,"EXTERNAL_ID":i,"INTERNAL_ID":v["INTERNAL_ID"],
                                    "CREATED_AT":t["CREATED_AT"],"ORG":t["ORG"],"USER":t["USER"],
                                    "PROCESS":v["PROCESS"],"SYSTEM":v["SYSTEM"],"GRAPH_ID":"GR000"+str(Trans.last_G_index+l),"STATUS":"Active",
                                    "GRAPHS":v["GRAPHS"],"PV":v["PV"],"DEP&LIAB_SCHEDULE":v["DEP&LIAB_SCHEDULE"],"HEADER":v["HEADER"],
                                    "LESSOR":v["LESSOR"],"ASSET_SCHEDULE":v["ASSET_SCHEDULE"],"DISC_RATE":v["DISC_RATE"],
                                    "APPROACH_SUMMARY":v["APPROACH_SUMMARY"],"PAYMENTS":v["PAYMENTS"],"INCEPTION/TRANSITION_TABLE":v["INCEPTION/TRANSITION_TABLE"],
                                    "LIABILITY_REVERSALS_TABLE":v["LIABILITY_REVERSALS_TABLE"],"RENT_POSTED_TABLE":v["RENT_POSTED_TABLE"],
                                    "DEPRECIATION/ACCUMULATED/DEPRECIATION_TABLE":v["DEPRECIATION/ACCUMULATED/DEPRECIATION_TABLE"],"COST/LIABILITY_TABLE":v["COST/LIABILITY_TABLE"]})
        Trans.index.replace_one({"GRAPH":"0"},{"GRAPH":["0",str(Trans.last_G_index+l)]})   
    else:
        data.fillna(method="ffill",inplace=True)
        data=data.astype({"LE_ExternalID":int,"LE_Pymt_Amt":int})
        data=data.astype({"LE_ExternalID":str,"LE_Pymt_Amt":str})
        Trans=AS116(data=data,process=process,organization="XYZ Company")
        G=Trans.BaseGraph()
        global decoder
        decoder,df=Trans.Preprocessing(Graph=G)
        Details=Trans.Trans_detail
        t=Trans.Trans_table.find_one({"TRANSACTION_ID":transaction_id,"STATUS":"In Progress"})
        global EXP
        EXP=Trans.GetExploder()
        l=0  
        global exploder
        for i in list(EXP.keys()):
            exploder={i:EXP[i]}
            stored_info=Details.find_one({"INTERNAL_ID":i,"EXTERNAL_ID":decoder[i],"STATUS":"Active"})
            if stored_info!=None:
                pass
                Details.update_many({"INTERNAL_ID":i,"EXTERNAL_ID":decoder[i]},{"$set":{"STATUS":"Active"}})
            l+=1 
            GU=Trans.EXPLODEGRAPH(Graph=G,Head=None,exploder=exploder)
            g1=Trans.AssignInputs()
            g2=Trans.AssignDerived()
            Pv1=Trans.GroupPV()
            Sch=Trans.ScheduleForJS()
            Head=Trans.HeaderForJS()
            Less=Trans.LessorForJS()
            Ass=Trans.AssetForJS()
            Summ=Trans.SummaryForJS()
            Pymt=Trans.PymtForJS()
            Disc=Trans.DiscForJS()
            Sch_node=Trans.FindNode(Graph=GU,key="LE",node="LE_Sch_Holder")
            table=pd.DataFrame(GU.nodes[Sch_node[0]]["TRUTH"])
            table.LE_Sch_Line_Calc_From=pd.to_datetime(table.LE_Sch_Line_Calc_From)
            table.LE_Sch_Line_Calc_To=pd.to_datetime(table.LE_Sch_Line_Calc_To)
            iD=decoder[i]
            G1=Trans.SubGraph(process_code="116A")
            G2=Trans.SubGraph(process_code="116B")
            G3=Trans.SubGraph(process_code="116C")
            G4=Trans.SubGraph(process_code="116D")
            G5=Trans.SubGraph(process_code="116E")
            SETUP1,INC_TRA=Trans.MiniProcess(ACC=None,Graph=G1,Id=iD,data=None,process_code="116A",table=table)
            SETUP2,DEP_ACC=Trans.MiniProcess(ACC=None,Graph=G2,Id=iD,data=None,process_code="116B",table=table)
            SETUP3,INT_LIA=Trans.MiniProcess(ACC=None,Graph=G3,Id=iD,data=None,process_code="116C",table=table)
            SETUP4,LIA_REV=Trans.MiniProcess(ACC=None,Graph=G4,Id=iD,data=None,process_code="116D",table=table)
            SETUP5,REN_POS=Trans.MiniProcess(ACC=None,Graph=G5,Id=iD,data=None,process_code="116E",table=table)
            G1.nodes["INDAS116_Lease_Accounting_Inception_Transition"]["TRUTH"]=SETUP1.to_json()
            G2.nodes["INDAS116_Lease_Accounting_Depreciation"]["TRUTH"]=SETUP2.to_json()
            G3.nodes["INDAS116_Lease_Accounting_Liabilities"]["TRUTH"]=SETUP3.to_json()
            G4.nodes["INDAS116_Lease_Accounting_Liability_Reversals"]["TRUTH"]=SETUP4.to_json()
            G5.nodes["INDAS116_Lease_Accounting_Rent_Posting"]["TRUTH"]=SETUP5.to_json()            
            GRAPHS={"SETUP":json_graph.node_link_data(g2),"116A":json_graph.node_link_data(G1),
                    "116B":json_graph.node_link_data(G2),"116C":json_graph.node_link_data(G3),
                    "116D":json_graph.node_link_data(G4),"116E":json_graph.node_link_data(G5)}
            processes=[Trans.process,"INDAS116_Lease_Accounting_Inception_Transition","INDAS116_Lease_Accounting_Depreciation","INDAS116_Lease_Accounting_Liabilities"]
            Details=Trans.DB["DETAILS_TABLE"]
            Details.insert_one({"TRANSACTION_ID":transaction_id,"EXTERNAL_ID":decoder[i],"INTERNAL_ID":i,"USER":t["USER"],"ORG":t["ORG"],"STATUS":"Active",
                                "CREATED_AT":t["CREATED_AT"],"PROCESS":processes,"SYSTEM":["Audire","ERP"],"GRAPH_ID":"GR000"+str(Trans.last_G_index+1),
                                "GRAPHS":GRAPHS,"PV":Pv1,"DEP&LIAB_SCHEDULE":Sch,"HEADER":Head,"LESSOR":Less,"ASSET_SCHEDULE":Ass,
                                "DISC_RATE":Disc,"APPROACH_SUMMARY":Summ,"PAYMENTS":Pymt,"INCEPTION/TRANSITION_TABLE":INC_TRA,
                                "LIABILITY_REVERSALS_TABLE":"NOT_YET_ADDED","RENT_POSTED_TABLE":"NOT_YET_ADDED",
                                "DEPRECIATION/ACCUMULATED/DEPRECIATION_TABLE":DEP_ACC,"COST/LIABILITY_TABLE":INT_LIA})
            Trans.index.replace_one({"GRAPH":"0"},{"GRAPH":["0",str(Trans.last_G_index+l)]}) 
    Trans.Trans_table.update_one({"TRANSACTION_ID":transaction_id,"STATUS":"In Progress"},{"$set":{"STATUS":"Completed"}})
    return {"PROCESS":"COMPLETED"};


class AfterResponse:
    def __init__(self, app=None):
        self.callbacks = []
        if app:
            self.init_app(app)

    def __call__(self, callback):
        self.callbacks.append(callback)
        return callback

    def init_app(self, app):
        # install extension
        app.after_response = self

        # install middleware
        app.wsgi_app = AfterResponseMiddleware(app.wsgi_app, self)

    def flush(self):
        for fn in self.callbacks:
            try:
                fn()
            except Exception:
                traceback.print_exc()

class AfterResponseMiddleware:
    def __init__(self, application, after_response_ext):
        self.application = application
        self.after_response_ext = after_response_ext

    def __call__(self, environ, after_response):
        iterator = self.application(environ, after_response)
        try:
            return ClosingIterator(iterator, [self.after_response_ext.flush])
        except Exception:
            traceback.print_exc()
            return iterator

app = Flask("after_response")
AfterResponse(app)

Inputs = []

@app.after_response
def after():
    global Inputs
    if len(Inputs)>0:
        for i in Inputs:
            try:
                R=json.dumps(CODEAS116(i))
            except 
            print(R)
            return R;

@app.route('/root', methods=['POST'])
def Input():
    global Inputs
    Inputs=json.loads(request.data)
    V=json.dumps({"Status":"PROCESS STARTED"})
    return V;

@app.route('/health', methods=['GET'])
def health():
    V=json.dumps({"status":"Online"})
    return V;




if __name__ == '__main__':
    app.run(host="0.0.0.0",port=7000,debug=True)


