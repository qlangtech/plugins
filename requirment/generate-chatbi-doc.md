需要帮我写一篇TIS基于本体语义层GraphRAG的ChatBI智能问数实现方案，文章需要包括如下部分：
1. 为什么提供chatBI智能问数的功能？主要是由于开发了本体语义层，光有一个本体没有实际价值，为了让本体模型有具体的使用场景，所以开发了ChatBI功能。还需要强调一下TIS ChatBI是开源的，一站式开箱即用的特点。
2. ChatBI架构流程介绍，组件交代清晰，如：本体、基于neo4j实现的GraphRAG，MCP服务端，TIS的数据集成。通过ChatBI从用户提出问题，到最终生成结果，如何将之前提到的各个功能组件串接在一起的。
3. 使用GraphRAG实现ChatBI比传统的chatBI智能问数的优势，特别介绍下本体中Glossary和Link Type在ChatBI中发挥的重要作用。
4. 介绍TIS ChatBI比开源领域其他产品的比较，优势是什么，要让用户直观感受到TIS的ChatBI的优势。
5. TIS ChatBI实际操作说明
    1. 从数据库中导出Object Type 到本体域中
    2. 自动创建语义层，以前这一步是一个需要细致且繁琐的工作，TIS中提供了通过大模型自动生成语义层资源的功能，大大提供了配置效率，后期用户只需要对生成的配置稍事修剪即可。
    3. 创建ChatBI Skill 技能，简要说明一下在EnableChatBI中各个配置对ChatBI最终结果的影响作用是什么。
    4. 到TIS后台通过/Users/mozhenghua/j2ee_solution/project/tis-console/src/base/ontology.chat-bi.query.component.ts 进行查询。也可以通过MCP服务/Users/mozhenghua/j2ee_solution/project/tis-solr/tis-console/src/main/java/com/qlangtech/tis/mcp/tools/ChatBITool.java到OpenClaw或者hermes 中与进行ChatBI交互，好处是还能利用到专业agent中其他功能价值，例如：定时任务、专业渲染组件对结果集渲染。
    5. 在后台查看ChatBI的/Users/mozhenghua/j2ee_solution/project/tis-solr/tis-plugin/src/main/java/com/qlangtech/tis/plugin/ontology/chatbi/TraceStep.java执行日志，可以明确了解ChatBI执行过程中执行的逻辑，可以有针对性地调整本体结构或者修改EnableChatBI中的配置，以提高ChatBI的准确率。
    6. 使用测试集测试结果说明，使用falcon的一个数据子集 /Users/mozhenghua/j2ee_solution/project/tis-solr/design/chat-bi/falcon/tool/doris_init_db_14.sql 进行测试，构建一个说明通过该测试集合测试获得了一个比较高的准确性
6. 对以上的总结

以上是文章的大纲，文章路径：/opt/misc/tis-docs2/docs/example/chat-bi/index.mdx

具体要求：
1. 该文章是给初次接触TIS的数据集从业人员看的，文章内容不要涉及过于具体的技术细节，文章中不要出现具体的代码脚本，以通俗易懂的科普风格
2. _category_.json 中的内容帮助编辑一下，"position": 属性是文档所在上下文优先级
3. 编写完以上内容后更新一下 /opt/misc/tis-docs2 项目的相关SEO内容
4. 有需要配图的地方请预留位置，用文字加以说明配图内容
5. 有关TIS 本体的内容请查阅 ontology memory
6. 说明中如需要添加超链接，请按照如下方式添加：
    1. 在文档头部添加： import Link from '/src/components/Link';
    2. 在文档内部添加链接标签，例如： <Link href={require("./plugin-develop-detail.mdx")}>插件开发说明</Link>