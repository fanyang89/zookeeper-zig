#!/usr/bin/env python3
import pathlib
import sys


def replace_once(source: str, old: str, new: str, label: str) -> str:
    count = source.count(old)
    if count != 1:
        raise SystemExit(f"expected one {label} insertion point, found {count}")
    return source.replace(old, new, 1)


def replace_region(source: str, start: str, end: str, replacement: str, label: str) -> str:
    start_index = source.find(start)
    if start_index < 0:
        raise SystemExit(f"missing {label} start marker")
    end_index = source.find(end, start_index + len(start))
    if end_index < 0:
        raise SystemExit(f"missing {label} end marker")
    return source[:start_index] + replacement + source[end_index:]


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {sys.argv[0]} PATH_TO_CLIENT_BASE_JAVA")

    path = pathlib.Path(sys.argv[1])
    source = path.read_text(encoding="utf-8")

    source = replace_once(
        source,
        "import org.slf4j.LoggerFactory;\n",
        "import org.slf4j.LoggerFactory;\n"
        "import top.fuis.zookeeperzig.interop.ZigServerProcess;\n",
        "interop import",
    )
    source = replace_once(
        source,
        "    protected boolean exceptionOnFailedConnect = false;\n",
        "    protected boolean exceptionOnFailedConnect = false;\n"
        "    private ZigServerProcess externalServerProcess = null;\n",
        "external process field",
    )
    source = replace_once(
        source,
        "                JMXEnv.ensureAll(getHexSessionId(zk.getSessionId()));\n",
        "                if (!useExternalServer()) {\n"
        "                    JMXEnv.ensureAll(getHexSessionId(zk.getSessionId()));\n"
        "                }\n",
        "client JMX assertion",
    )
    source = replace_region(
        source,
        "    private void startServer(int serverId) throws Exception {\n",
        "    private void verifyUnexpectedBeans(Set<ObjectName> children) {\n",
        "    private void startServer(int serverId) throws Exception {\n"
        "        if (useExternalServer()) {\n"
        "            externalServerProcess = ZigServerProcess.start(\n"
        "                System.getProperty(\"zookeeper.zig.server\"),\n"
        "                serverId,\n"
        "                getPort(hostPort),\n"
        "                tmpDir,\n"
        "                CONNECTION_TIMEOUT);\n"
        "            return;\n"
        "        }\n"
        "        LOG.info(\"STARTING server\");\n"
        "        serverFactory = createNewServerInstance(serverFactory, hostPort, maxCnxns);\n"
        "        startServerInstance(tmpDir, serverFactory, hostPort, serverId);\n"
        "        Set<ObjectName> children = JMXEnv.ensureParent(\"InMemoryDataTree\", \"StandaloneServer_port\");\n"
        "        verifyUnexpectedBeans(children);\n"
        "    }\n\n"
        "    private boolean useExternalServer() {\n"
        "        return System.getProperty(\"zookeeper.zig.server\") != null;\n"
        "    }\n\n",
        "startServer method",
    )
    source = replace_once(
        source,
        "        if (tmpDir != null) {\n"
        "            assertTrue(recursiveDelete(tmpDir), \"delete \" + tmpDir.toString());\n"
        "        }\n",
        "        if (tmpDir != null && !useExternalServer()) {\n"
        "            assertTrue(recursiveDelete(tmpDir), \"delete \" + tmpDir.toString());\n"
        "        }\n",
        "external server log preservation",
    )
    source = replace_region(
        source,
        "    protected void stopServer() throws Exception {\n",
        "    protected void tearDownAll() throws Exception {\n",
        "    protected void stopServer() throws Exception {\n"
        "        LOG.info(\"STOPPING server\");\n"
        "        if (useExternalServer()) {\n"
        "            if (externalServerProcess != null) {\n"
        "                externalServerProcess.close();\n"
        "                externalServerProcess = null;\n"
        "            }\n"
        "            return;\n"
        "        }\n"
        "        shutdownServerInstance(serverFactory, hostPort);\n"
        "        serverFactory = null;\n"
        "        JMXEnv.ensureOnly();\n"
        "    }\n\n",
        "stopServer method",
    )

    path.write_text(source, encoding="utf-8")


if __name__ == "__main__":
    main()
