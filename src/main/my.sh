#!/bin/bash

# 2021-02-04
# Author: dw_chenguanhong
# 用于检查进程接入ali Arthas诊断agent,并且生成服务_arthas端口对应配置文件
# 查找进程pid
# 扫描监听端口
# 部署arthas/生成配置文件/删除过期配置

# Test Parameter
set -x

this_script=$0
# service_key_ori=$1
#ip=$2
# arthas_download=$3
# pod_name=$4
# run_user=$5
script_argv="$*"

service_key=${service_key_ori//@/____}
run_type=$(echo "${service_key}" | awk -F'____' '{print $1}')
service_name=$(echo "${service_key}" | awk -F'____' '{print $2"____"$3}')

s_dev_key="9fPFWfMvEQx6CYfGapWf"
s_dev_user="dw_fanhongcheng"
agent_config_id="155077"

arthas_jar_root=/data/webapps/arthas
pod_script_path="/tmp/${this_script##*/}"
arthas_agent_service_key="1@sysop_arthas_agent"

default_http_port=8563
default_telnet_port=3658
defalut_arthas_server_port=7777
default_arthas_server_domain="arthas-tunnel-server.sysop.duowan.com"
# default_arthas_server="ws://${default_arthas_server_domain}:${defalut_arthas_server_port}/ws"
default_arthas_server="ws://${default_arthas_server_domain}/ws"



# [ -z "${arthas_download}" ] && arthas_download="http://${default_arthas_server_domain}:1998/arthas_agent.tar.gz"
[ -z "${arthas_download}" ] && arthas_download="https://arthas-download.sysop.duowan.com/arthas_agent.tar.gz"

if [ -z "$(which kubectl)" ];then
    kubectl_bin="/usr/local/bin/kubectl --kubeconfig /root/.kube/config"
else
    kubectl_bin="kubectl --kubeconfig /root/.kube/config"
fi

#
if [ -f /usr/local/java/bin/java ];then
    java_path="/usr/local/java/bin/java"
else
    java_path=$(which java)
fi

function logSuc() {
    argv=($@)
    func_name=${argv[0]}
    ret_code=0
    msg="${argv[@]:1}"
    echo -e "$(date +'%F %T') [I] [${service_key}] [$ip] [${func_name}] : ${msg}" >&2
    exit ${ret_code}
}

function logFailed(){
    argv=($@)
    func_name=${argv[0]}
    ret_code=${argv[1]}
    msg="${argv[@]:2}"
    echo -e "$(date +'%F %T') [E] [${service_key}] [$ip] [${func_name}] : ${msg}" >&2
    exit ${ret_code}
}

function logDebug() {
    argv=($*)
    func_name=$1
    msg="${argv[@]:1}"
    echo -e "$(date +'%F %T') [D] [${service_key}] [$ip] [${func_name}] : ${msg} " >&2
}

function parseIspIp() {
    ip_list=$(cat /home/dspeak/yyms/hostinfo.ini | grep ip_isp_list | cut -d '=' -f2 | sed 's/,/\n/g')
    for ip_isp in ${ip_list};do
        eval "export ${ip_isp##*:}_IP=${ip_isp%%:*}"
    done
}

parseIspIp

function getIspIp() {
    isp=$1
    eval "echo \${${isp}_IP}"
}

function getServicePid() {
    pids=$(ps aux | grep -Ewv 'grep|ssh|bash' | grep -E "Ddragon.bizName.projName=${service_name} -Ddragon.businessDomain" \
    | awk '{print $2}')
    echo "${pids}"
    logDebug $FUNCNAME "Get services pid"
}

function containerGetJavaPid() {
    pids=$(ps aux | grep -Ewv 'grep|ssh|bash' | grep -w "java " | awk '{print $2}')
    echo "${pids}"
    logDebug $FUNCNAME "Get services pids: ${pids}"
}

function getAllListenPortsIno(){
    local listen_port_ino=$(netstat -tulne | awk '{print $4,$NF}' | awk -F':' '{print $NF}' | grep -v '[A-Za-z]')
    logDebug $FUNCNAME "all listen port ino: ${listen_port_ino}"
    echo "${listen_port_ino}"
}

function getAllPidsSocketIno(){
    local pids="$1"
    socketInfo=""
    for pid in ${pids};do
        info=$(ls -l /proc/${pid}/fd | grep -o socket:.*)
        socketInfo="${socketInfo}${socketInfo:+\n}${info}"
    done
    logDebug $FUNCNAME "all pids: ${pids} socket info: ${socketInfo}"
    echo -e "${socketInfo}"
}

function getPidsPorts() {
    local pids=$1
    local outputPorts
    listen_ports_ino=$(getAllListenPortsIno) # port ino\n
    pids_socket_ino=$(getAllPidsSocketIno "${pids}")
    while read -a tuple;do
        port=${tuple[0]}
        ino=${tuple[1]}
        echo "${pids_socket_ino}" | grep -wq "${ino}"
        [ $? -eq 0 ] && outputPorts=(${outputPorts[@]} ${port})
    done < <(echo "${listen_ports_ino}")
    logDebug $FUNCNAME "get ${service_key} listen port: ${outputPorts[@]}"
    echo "${outputPorts[@]}"
}

function isApiPort() {
    local port="$1"
    output=$(timeout 3 wget -qO - "http://$(getIspIp "INTRANET"):${port}/api" --post-data='{
    "action":"exec",
    "command":"session"
    }')
    rst=$(echo "${output}" | json_pp 2>/dev/null)
    if [ $? -eq 0 ];then
        agent_id=$(echo "${output}" | json_pp | grep 'agentId' | cut -d'"' -f4)
        return 0
    else
        return 1
    fi
}

function HttpGet() {
    local url=$1
    local output_path=$2
    local output_file_name=$3
    mkdir -p ${output_path}/log
    for (( i = 0; i < 3; ++i )); do
        wget -q ${url} --output-document=${output_path}/${output_file_name} \
        --output-file=${output_path}/log/download_$(date +"%F_%T").log --continue
        [ $? -eq 0 ] && logDebug $FUNCNAME "Download from ${url} successes , save in ${output_path}/${output_file_name} " \
        && return 0
    done
    logDebug $FUNCNAME "Failed to download from ${url}, see log in ${output_path}/log/download_*"
    return 1
}

function GetIdelPort() {
    local i
    telnet_port=$(( ${http_port:-$default_telnet_port} + 1 ))
    tel_array=( $(echo ${telnet_port} | sed 's/\([0-9]\)/\1 /g') )
    http_port=""
    for (( i = 1; i < ${#tel_array[@]}+1; ++i )); do
        http_port="${http_port}${tel_array[-$i]}"
    done
    logDebug $FUNCNAME "use http port: ${http_port}, telnet_port: ${telnet_port}"
}

function is_default_port_idle() {
    is_port_idle ${default_http_port} && is_port_idle ${default_telnet_port}
}

function is_port_idle(){
    local port="$1"
    rst=$(lsof -i:$port)
    [ -z "${rst}" ]
}

function getPkgLatestVer() {
    # 通过接口获取包最新
    local serviceKey="$1"
    local url="http://s-backend.sysop.duowan.com/release/api/das/version/getNewestInstanceVersion?serviceKey=${serviceKey}&key=${s_dev_key}&userName=${s_dev_user}"
    python_bin=$(which python)
    [ -z "${python_bin}" ] && python_bin=/usr/bin/python
    latest_version=$(python <<EOF
#!/usr/bin/python
import urllib2
import json
req = urllib2.Request("${url}")
rsp = urllib2.urlopen(req,timeout=5)
jstr = rsp.read()
data = None
try:
    data = json.loads(jstr)["data"]
except Exception as e:
    print("Failed to loads jstr: %s" % jstr)
    exit(1)
# latest_ver=None
# for ver in data:
#    if latest_ver=None:
#        latest_ver=ver
#        continue
#    if ver["id"] > latest_ver["id"]
#        latest_ver=ver
if data:
    print(data["id"])
EOF
)
    [ $? -eq 0 ]  && echo "${latest_version}"
}

function install_pkg() {
    local service_key="$1"
    local install_ver="$2"
    local install_ip="$3"
    local timeout="60"
    local url="http://s-backend.sysop.duowan.com/release/api/das/deployment/install"
    rst=$(curl "${url}" -H 'Content-Type: application/json' -d "{
    \"userName\": \"${s_dev_user}\",
    \"key\": \"${s_dev_key}\",
    \"action\": {
         \"serviceKey\": \"${service_key}\",
         \"ips\":[\"${install_ip}\"],
         \"autoStartUp\":1,
         \"pubType\":1,
         \"configId\":${agent_config_id},
         \"versionId\":${install_ver},
         \"timeout\":${timeout},
         \"executeType\":1,
         \"interval\":5,
         \"interrupt\":1,
         \"env\":1
    }
}")
    echo "${rst}" | json_pp | grep -q '"code"\s*:\s*0'
    if [ $? -eq 0 ];then
        taskId=$(echo "${rst}" | json_pp | grep 'taskId' | cut -d'"' -f4)
        sessionId=$(echo "${rst}" | json_pp | grep 'sessionId' | cut -d':' -f2 | grep -o '[0-9]*')
        echo "${taskId}:${sessionId}"
    fi
}

function queryInstallTask() {
    local taskId="$1"
    local sessionId="$2"
    local url=" http://s-backend.sysop.duowan.com/release/api/das/jobTask/listDetailById?taskId=${taskId}&key=${s_dev_key}&userName=${s_dev_user}&queryParam=false"
    python_bin=$(which python)
    [ -z "${python_bin}" ] && python_bin=/usr/bin/python
    python << EOF
#!/usr/bin/python
# -*- coding: utf-8 -*
import urllib2
import json
import time
req = urllib2.Request("${url}")
for i in range(0,10):
    time.sleep(5)
    try:
        rsp = urllib2.urlopen(req, timeout=5)
    except Exception as e:
        print("Failed to open url: %s , err: %s" % (url, str(e)))
        continue
    jstr = rsp.read()
    try:
        data = json.loads(jstr)['data']
    except Exception as e:
        print("Failed to loads json: %s" % jstr)
        continue
    task_status=data.setdefault('jobTask',{}).setdefault('status',0)
    if task_status == 1:
        pub_status = data.setdefault('pubServerLogs',[{}])[0].setdefault('status',-1)
        if pub_status == 0:
            print("install arthas agent success!!")
            exit(0)
        elif pub_status == 1:
            continue
        else:
            exit(pub_status)
exit(3)
EOF
# 机器的发布状态   ： -1初始化, 0成功，1执行中，2失败，3超时,4无效ip，5 任务中断，6任务冲突,7 人工停止, 8异常 50-200脚本返回失败码
    ret=$?
    return $ret
}

function downloadArthasInPod() {
    # 在容器中下载arthas
    mkdir -p ${arthas_jar_root}/bin
    mkdir -p ${arthas_download}
    HttpGet ${arthas_download} ${arthas_jar_root} "arthas_agent.tar.gz"
    ## test
    # ls ${arthas_jar_root} && exit 0
    #
    tar -xvf "${arthas_jar_root}/arthas_agent.tar.gz" -C ${arthas_jar_root}/bin
}

function downloadArthasInPhysics() {
    # 在物理机中下载arthas
    install_version=$(getPkgLatestVer "${arthas_agent_service_key}")
    ## test
    # echo "${install_version}" && exit 0
    #
    [ -z "${install_version}" ] && logFailed $FUNCNAME 253 "Can not get the latest version of package: ${arthas_agent_service_key}"
    task_id_session_id=$(install_pkg "${arthas_agent_service_key}" "${install_version}" "${ip}")
    ## test
    # echo "${task_id_session_id}" && exit 0
    #
    [ -z "${task_id_session_id}" ] && logFailed $FUNCNAME 252 "Can not create install task to s-dev: ${arthas_agent_service_key}"
    task_id="${task_id_session_id%%:*}"
    session_id="${task_id_session_id##*:}"
    queryInstallTask "${task_id}" "${sessionId}"
    ret=$?
    ## test
    # echo "${ret}" && exit 0
    #
    case $ret in
    0)
       logDebug "$FUNCNAME" "install arthas success"
       ;;
    *)
       logFailed "$FUNCNAME" 251 "install arthas failed, rst code: $ret"
       ;;
    esac
}

function StartExternalArthas() {
    local arthas_server_url="$1"
    # 如果环境中不存在arthas_agent
    logDebug $FUNCNAME "Check if the agent jar had installed..."
    if [ ! -f "${arthas_jar_root}/bin/arthas-boot.jar" ];then
    # 下载arthas agent
        logDebug $FUNCNAME "Start to download arthas agent jar "
        if isInPhysics;then
            downloadArthasInPhysics
        elif isInContainer;then
            downloadArthasInPod
        else
            logFailed "${FUNCNAME}" 252 "Failed to get env"
        fi
    fi
    ## test
    # exit 0
    #
    # 启动arthas 进程
    logDebug $FUNCNAME "Arthas agent: Start to Attach process..."
    cd ${arthas_jar_root}/bin
    port_valid=1
    if is_default_port_idle;then
        ## test
        #echo "use default port, default_telnet_port: ${default_telnet_port}, default_http_port: ${default_http_port}, \
#arthas_server_url: ${arthas_server_url}, " && exit 0
        #
        port_valid=0
        run_user=$(ps -ef | awk -v awkVar=${service_pids} '{ if($2 == awkVar){print $1} }')
        su ${run_user} -c "${java_path} -jar arthas-boot.jar \
        --tunnel-server "${arthas_server_url}" \
        --target-ip $(getIspIp "INTRANET") \
        --attach-only \
         --telnet-port ${default_telnet_port}\
        --http-port ${default_http_port}\
        --arthas-home ${arthas_jar_root}/bin ${service_pids};exit"
    else
        for (( j = 0; j < 20; ++j )); do
            GetIdelPort
            if is_port_idle ${http_port}  && is_port_idle ${telnet_port};then
                port_valid=0
                break
            fi
        done

        ## test
        # echo "use port, api_port: ${http_port}, telnet port: ${telnet_port}" && exit 0
        #

        [ ${port_valid} -ne 0 ] && logDebug $FUNCNAME "No resource of port" && exit 1
        su ${run_user} -c "${java_path} -jar arthas-boot.jar \
        --tunnel-server ${arthas_server_url} \
        --target-ip $(getIspIp "INTRANET") \
        --attach-only \
        --telnet-port ${telnet_port} \
        --http-port ${http_port} \
        --arthas-home ${arthas_jar_root}/bin ${service_pids}"
    fi

    if isApiPort "${http_port:-${default_http_port}}";then
        return 0
    else
        logDebug $FUNCNAME "start arthas agent failed: http_port: ${http_port:-${default_http_port}}, telnet_port: \
${telnet_port:-${default_telnet_port}}, tunnel-serve: ${arthas_server_url}"
        return 255
    fi
}

function recordToConfig() {
    # 将service_key____telnet_port____http_port记录下来
    mkdir -p ${arthas_jar_root}/config
    config_files="${arthas_jar_root}/config/${service_key_ori}*"
    [ -n "${config_files}" ] && rm -f ${config_files}
    local config=${arthas_jar_root}/config/${service_key_ori}____${telnet_port:-${default_telnet_port}}____${http_port:-${default_http_port}}____${agent_id}
    touch ${config}
    [ -f ${config} ] && logDebug $FUNCNAME "created service arthas config file: ${config}" && return 0
    return 1
}

function physicsMain() {
    service_pids=( $(getServicePid) )
    ## test
    # echo "${service_pids[@]}" && exit 0
    ##
    [ -z "${service_pids[@]}" ] && logFailed $FUNCNAME "find no proc about ${service_key}"
    service_listen_ports=$(getPidsPorts ${service_pids[@]})
    ## test
    # echo "${service_listen_ports}" && exit 0
    ##
    api_port=""
    for port in ${service_listen_ports};do
        if isApiPort $port;then
            api_port=${port}
            logDebug $FUNCNAME "isApiPort" "get api port: $api_port"
            break
        fi
    done
    ## test
    # echo "${api_port}" && exit 0
    ##
    if [ -z "${api_port}" ];then
        StartExternalArthas ${arthas_telnet_server:-${default_arthas_server}}
        if [ $? -eq 0 ];then
            recordToConfig && logSuc $FUNCNAME "Success to start arthas"
            logFailed $FUNCNAME 255 "Failed to record Arthas"
        else
            logFailed $FUNCNAME 255 "Failed to start external Arthas"
        fi
    else
        http_port="${api_port}"
        htt_array=( $(echo ${http_port} | sed 's/\([0-9]\)/\1 /g') )
        telnet_port=""
        for (( i = 1; i < ${#htt_array[@]}+1; ++i )); do
            telnet_port="${telnet_port}${htt_array[-$i]}"
        done
        recordToConfig
        logSuc $FUNCNAME "Record Success"
    fi
}


## container run interface:
# isPhysics: 判断当前执行环境是否是在物理机 返回值：0/非0
# isContainer: 判断当前执行环境是否在容器 返回值: 0/非0
# searchPod: 实现定位具体pod的方式 赋值变量podList (需要自定义)
# insertIntoContainer: 将当前脚本注入到podList变量指定的pod中，输入/读取变量：podList 赋值变量：podScriptName 返回值: 0/非0 (内部方法，不建议重写)
# executeInContainer: 在podList中执行注入的脚本
# containerMain: 在容器中执行的main方法 (自定义)
# RunInContainer: 外部调用实现容器执行的入口 (不建议自定义)

function isInPhysics() {
     [ -z "${MY_POD_IP}" ]
}

function isInContainer() {
    [ -n "${MY_POD_IP}" ]
}

function SearchPod() {
    # 获取pod的方式
    p_p=( ${service_key//____/ } )
    product_name=${p_p[1]}
    project_name=${p_p[2]}
    ## test
    # echo "pd: ${product_name} pj: ${project_name}" && exit 0
    #
    pod_infos=$(${kubectl_bin} get pod --no-headers -l project-name=${project_name} -n ${product_name} -o wide)
    # test
    # echo "${pod_info}"
    #
    # 优先使用pod_name进行搜索
    if [ -n "${pod_name}" ];then
        podList=$(echo "${pod_infos}" | grep -w ${pod_name} | awk '{print $1}' |xargs)
    else
        podList=$(echo "${pod_infos}" | grep -w ${ip} | awk '{print $1}' |xargs)
    fi
}

function insertIntoContainer() {
    # 将脚本注入pod列表的容器中
    for pod in ${podList};do
        ## test
        # echo "${kubectl_bin} cp ${this_script} ${pod}:${pod_script_path} -n ${product_name}" && exit 0
        #
        ${kubectl_bin} cp ${this_script} ${pod}:${pod_script_path} -n ${product_name}
        if [ $? -eq 0 ];then
            logDebug  $0 "Insert script into pod ${pod} success"
        else
            logFailed $FUNCNAME 254 "Insert script into pod ${pod} failed"
        fi
    done
}

function searchContainer() {
    local pod="$1"
    local namespace="$2"
    local containerList
    local json_path='{range .status.containerStatuses[*]}{.name}{" "}'
    if [ -z "${namespace}" ];then
        containerList=$(${kubectl_bin} get pod ${pod} -A -o jsonpath="${json_path}")
    else
        containerList=$(${kubectl_bin} get pod ${pod} -n ${namespace} -o jsonpath="${json_path}")
    fi
    logDebug $FUNCNAME "get pod $pod containers: ${containerList}"
    echo "${containerList}"
}

function executeInContainer() {
    # 在容器内执行脚本
    local containerList
    for pod in $podList;do
        containerList=$(searchContainer ${pod} ${product_name})
        for container in ${containerList};do
            ## test
            # echo "${kubectl_bin} exec $pod -n ${product_name} -c ${container} -it /bin/bash ${pod_script_path} ${script_argv}" && exit 0
            #
            ${kubectl_bin} exec $pod -n ${product_name} -c ${container} -it /bin/bash ${pod_script_path} ${script_argv}
            rst_code=$?
            [ ${rst_code} -eq 0 ] && logDebug $FUNCNAME "execute script in pod ${pod}, container: ${container} success code: ${rst_code}" \
            && return 0
        done
    done
    return 254
}

function RunInContainer() {
    # 判断是否有kubectl命令
    # 根据pod ip查找具体的pod
    # 通过kubectl执行脚本注入
    # 通过kubectl执行脚本

    if  isInContainer;then
        ## test
        # echo "in container " && exit 0
        #
        containerMain
    elif isInPhysics;then
        version=$( ${kubectl_bin} version )
        ## test
        # echo "${version}" && exit 0
        #
        if [ $? -ne 0 ] || [ -z "${version}" ];then
            logFailed $FUNCNAME 254 "No kubectl command found"
        fi
        ## test
        # echo "in physics" && exit 0
        #
        SearchPod
        ## test
        # echo "${podList}" &&exit 0
        #
        insertIntoContainer
        ## test
        # echo $? && exit 0
        #
        executeInContainer
    fi
}


function containerMain() {
    # 获取所有监听端口
    # 获取所有java进程的socket 格式: pid socket
    # 扫描出java属于java进程的端口
    # 扫描这些端口是否有启动arthas进程
    # 没有的情况下安装
    service_pids=( $(containerGetJavaPid) )
    ## test
    # echo ${service_pids} && exit 0
    #
    [ -z "${service_pids}" ] && logFailed $FUNCNAME "find no proc about ${service_key}"
    service_listen_ports=$(getPidsPorts ${service_pids[@]})
    ## test
    # echo ${service_listen_ports} && exit 0
    #
    api_port=""
    for port in ${service_listen_ports};do
        if isApiPort $port;then
            api_port=${port}
            logDebug $FUNCNAME "isApiPort" "get api port: $api_port"
            break
        fi
    done
    ## test
    # echo "${api_port}" && exit 0
    #

    if [ -z "${api_port}" ];then
        StartExternalArthas ${arthas_telnet_server:-${default_arthas_server}}
        if [ $? -eq 0 ];then
            recordToConfig && logSuc $FUNCNAME "Success to start arthas"
            logFailed $FUNCNAME 255 "Failed to record Arthas"
        else
            logFailed $FUNCNAME 255 "Failed to start external Arthas"
        fi
    else
        http_port="${api_port}"
        htt_array=( $(echo ${http_port} | sed 's/\([0-9]\)/\1 /g') )
        telnet_port=""
        for (( i = 1; i < ${#htt_array[@]}+1; ++i )); do
            telnet_port="${telnet_port}${htt_array[-$i]}"
        done
        recordToConfig
        logSuc $FUNCNAME "Record Success"
    fi
    #
}

## container run interface


# 判断service_key开头,2为物理机潜龙,3为容器
function main() {
    if [ "${run_type}" -eq 2 ];then
        # 物理机主函数
        [ -z "${run_user}" ] && run_user="www-data"
        physicsMain
    elif [ "${run_type}" -eq 3 ];then
        # 容器主函数
        [ -z "${run_user}" ] && run_user="root"
        RunInContainer
    else
        echo "Can not get type of the ip: ${ip},  service_key: ${service_key}"
    fi
}

main
