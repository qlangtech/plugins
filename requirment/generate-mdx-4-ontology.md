# 核心需求
本需求是为这次版本开发中开发的ontology（本体）语意层中各个功能模块编写说明文档，生成的文档存放在/opt/misc/tis-docs2 工程中。

所有关于本体的说明文档存放在：/opt/misc/tis-docs2/docs/guide/ontology 目录下

# 编写 ontology 文档

以下是ontology 目录树结构说明，mdx文件旁边有 文件的作用说明，需要您帮我把文件内部的内容补全

文档目录结构说明

├── _category_.json
├── index.mdx 本体在TIS的作用，功能说明，意义 ，可参考： /Users/mozhenghua/Downloads/www.palantir.com_docs_foundry_ontologies_ontologies-overview_.png
├── link-type
│   ├── _category_.json
│   └── create-link-type.mdx 创建 link type功能说明，需要按照步骤说明，步骤1: 基本设置：/Users/mozhenghua/j2ee_solution/project/plugins/tis-ontology-plugin/src/main/java/com/qlangtech/tis/plugin/ontology/impl/linker/RelationshipTypeSetter.java
                                                                        步骤2:  可设置三种不同类型的link 1.RelationshipTypeObjectTypeForeignKeys 2.RelationshipTypeJoinTableDataset 3.RelationshipTypeBackingObjectType 需要说明三种类型的区别
│   └── edit-link-type.mdx 编辑 Link Type功能说明
│   └── index.mdx ,link type 的功能作用说明，内容可参考：/Users/mozhenghua/Downloads/www.palantir.com_docs_foundry_object-link-types_link-types-overview_.png
├── object-type
│   ├── _category_.json
│   ├── create-object.mdx， 创建object Type的说明文档 可参考：/Users/mozhenghua/Downloads/www.palantir.com_docs_foundry_object-link-create-object-type.png
│   ├── edit-object.mdx， 编辑 Object Type的说明文档
│   └── index.mdx ，Object Type的说明，内容可参考：/Users/mozhenghua/Downloads/www.palantir.com_docs_foundry_object-link-object-type.png
├── properties
│   ├── _category_.json
│   ├── add-property.mdx ， 添加 Property 的说明，参考 /Users/mozhenghua/j2ee_solution/project/plugins/tis-ontology-plugin/src/main/java/com/qlangtech/tis/plugin/ontology/impl/objtype/DefaultOntologyProperty.java
│   ├── derived-property.mdx 参考 /Users/mozhenghua/j2ee_solution/project/plugins/tis-ontology-plugin/src/main/java/com/qlangtech/tis/plugin/ontology/impl/role/MeasureRole.java，/Users/mozhenghua/Downloads/www.palantir.com_docs_foundry_object-link-types_derived_properties.png
│   ├── index.mdx 本体Object Type的的Property属性说明，参考：/Users/mozhenghua/Downloads/www.palantir.com_docs_foundry_object-link-Properties.png
│   ├── physical-expression.mdx 关于 /Users/mozhenghua/j2ee_solution/project/tis-solr/tis-plugin/src/main/java/com/qlangtech/tis/plugin/ontology/OntologyProperty.java:L73 physicalExpression 属性功能，使用场景说明
│   ├── role-type.mdx DefaultOntologyProperty.java:L79 roleType 的说明 功能及使用场景说明
│   ├── setting-shared-object.mdx，说明如何将一个属性与已经存在的Shared Object进行关联
│   └── setting-value-type.mdx ，说明如何将一个属性与已经存在的Value Type进行关联 
├── shared-property
│   ├── _category_.json
│   ├── edit-shared-property.mdx，创建、编辑Shared Property 进行说明
│   └── index.mdx ，对Shared Property 进行说明，参考：/Users/mozhenghua/Downloads/www.palantir.com_docs_foundry_object-link-types_shared-property-overview.png
└── value-type
    ├── _category_.json
    ├── edit-value-type.mdx ，创建、编辑 Vale Type进行说明
    └── index.mdx ，对value type进行说明，参考：/Users/mozhenghua/Downloads/www.palantir.com_docs_foundry_object-link-types_value-types-overview.png

要求：
1. 以上提供的截图都是从palantir 官网的截图，内容是英文的，你撰写的文档需要是**中文**的，palantir官网中关于本体中的对象的介绍可能与TIS中的实现方式有不一致的地方，所有内容必须以TIS现有实现的内容版本为准，不能有夸大虚假的内容。行文风格、内容可以借鉴palantir截图内容。
2. _category_.json 中的内容帮助编辑一下，"position": 属性是文档所在上下文优先级
3. 编写完以上内容后更新一下 /opt/misc/tis-docs2 项目的相关SEO内容
4. 有需要配图的地方请预留位置，用文字加以说明配图内容
5. 有关TIS 本体的内容请查阅 ontology memory
6. 说明中如需要添加超链接，请按照如下方式添加：
   1. 在文档头部添加： import Link from '/src/components/Link';
   2. 在文档内部添加链接标签，例如： <Link href={require("./plugin-develop-detail.mdx")}>插件开发说明</Link>