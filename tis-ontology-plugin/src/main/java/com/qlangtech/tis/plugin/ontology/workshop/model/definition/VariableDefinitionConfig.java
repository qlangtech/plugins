package com.qlangtech.tis.plugin.ontology.workshop.model.definition;

import com.google.common.collect.Maps;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.ontology.workshop.enums.VariableType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Workshop Variable 定义配置抽象基类
 * 定义变量值的计算方式，8 种具体子类对应不同的计算策略
 *
 * <h3>类型身份由类本身承担</h3>
 * 本体系<b>不再</b>有与之平行的 {@code VariableDefinitionType} 枚举（已删除）。
 * 原先 {@code WorkshopVariable} 上有一个 {@code definitionType} 字段与该枚举配合，
 * 但它是重复的分类体系：枚举的 7 个常量与当时的 7 个子类一一对应，中文标签也和各子类
 * {@code DefaultDescriptor.shortComment()} 逐字重复，并且作为一个独立的 SELECTABLE
 * 表单项与 {@code definitionConfig} 自身渲染出的 impl 选择器构成了同一个控件的两份。
 * 更糟的是它允许出现自相矛盾的状态（例如 {@code ObjectSetDefinitionConfig} 实例的
 * type 字段却是 {@code SQL_QUERY}），而编译器无从约束。
 *
 * <p>现在「值怎么算」完全由具体子类的 Java 类型表达，与 TIS 中其他 Describable
 * 多态体系（如 {@link com.qlangtech.tis.plugin.ontology.workshop.model.overlay.OverlayTypeConfig}）
 * 保持一致：
 * <ul>
 *   <li><b>持久化</b>：XStream 对声明类型为抽象类的字段会写入具体类（impl identity），
 *       无需额外的类型字段</li>
 *   <li><b>前端</b>：实例 JSON 上带扁平 {@code impl} 键（{@code AttrValMap.PLUGIN_EXTENSION_IMPL}），
 *       前端据此推导行为，参见 {@code widget-renderer.component.ts} 中
 *       {@code setupConfig.impl} 的用法</li>
 *   <li><b>表单</b>：{@code @FormField} 标注在 Describable 类型字段上时，TIS 原生生成
 *       impl 选择器（见 {@code PropertyType.isDescribable()}），各子类 descriptor 的
 *       {@code shortComment()} 会自动下发作为选项说明</li>
 * </ul>
 *
 * <h3>新增一种定义方式的改动面</h3>
 * <b>类本身</b>只需两步：新增子类、其 {@code DefaultDescriptor}；再有 {@code .json} 资源
 * 也仅是当子类里有 {@code @FormField} 时才需要——json 是给字段写 label/help 的，
 * 零字段的子类无处可写，参见 {@link ExternalInputConfig} 与同仓库的 {@code ActionButtonWidget}。
 * 漏改这三者会导致编译失败（{@link #isBackendComputed()} 是抽象方法），而不是运行时才暴露。
 *
 * <p><b>但整体改动面不止于此</b>，还有两处不在此文件的登记动作，且都<b>没有</b>编译期保护：
 * <ol>
 *   <li>把 descriptor 登记进 {@link #TYPE_DEFINITIONS}（见下节末段）</li>
 *   <li>前端 tis-console 的 {@code variable.service.ts} 为新的 kind 加
 *       {@code computeValueInternal} 分支——漏了会让<b>整个模块加载失败</b>
 *       （该方法 {@code default} 抛异常，而初始化会急切计算全部非 lazy 变量），
 *       以及 {@code variable.model.ts} 的 kind 联合类型</li>
 * </ol>
 *
 * <h3>与变量类型的兼容性（{@link #TYPE_DEFINITIONS}）</h3>
 * 变量类型（{@link VariableType}）与定义方式是<b>多对多</b>的：只有一部分定义方式能产出
 * 某一类值（例如「对象集合定义」只能产出 {@code OBJECT_SET}，而「函数计算」能产出绝大多数类型）。
 * 这条约束驱动了 Workshop 新建变量时的<b>两级级联菜单</b>——一级选变量类型，二级只列可用的
 * 定义方式。Palantir 官方文档 <i>concepts-variables.md</i> 的
 * "Variable definition type" 一节明文写了这条规则：
 * <pre>
 *   Available choices will vary based on the selected variable type
 * </pre>
 *
 * <p>该约束表达为 {@link #TYPE_DEFINITIONS} 这张映射表，消费方是
 * {@link #supports(VariableType, Descriptor)}（{@code WorkshopVariable} 落盘前校验）
 * 与 {@link #applicableDefinitions(VariableType, List)}（表单侧 {@code subDescEnumFilter}
 * 过滤候选 impl，由每个变量类型子类的 descriptor 注入）。
 *
 * <p><b>这不是被删掉的 {@code definitionType} 字段的回归</b>，两者形态根本不同：
 * <ul>
 *   <li>旧的 {@code definitionType} 是 <b>1:1 平行分类</b>（7 个枚举常量 ↔ 当时的 7 个子类，
 *       标签逐字重复），且作为<b>实例字段</b>落盘，允许出现自相矛盾的值</li>
 *   <li>{@link #TYPE_DEFINITIONS} 是 <b>12×8 的多对多能力声明</b>——这一维旧字段根本
 *       表达不了；它也不落在实例上（实例仍只有 {@code definitionConfig} 一个字段，
 *       没有冗余类型），只存在于描述符侧的静态映射表里</li>
 * </ul>
 *
 * <h3>矩阵的口径</h3>
 * 两张来源文档（官方 {@code concepts-variables.md} 每个类型的 "Initialized from ..." 散文，
 * 与本项目 {@code 03-variable-system.md} 附录 A「变量类型兼容性表」）在若干行上互相矛盾
 * （例如官方明说 SQL query 不能产出 object set / geopoint / geoshape，附录 A 却给
 * object-set 列了 sql-query）。本表按两条口径取值：
 * <ol>
 *   <li><b>取两者交集</b>（最保守）——只保留两份都认可的组合</li>
 *   <li><b>Function 严格按官方逐类型限定</b>——{@code GEOPOINT} / {@code OBJECT_SET_FILTER} /
 *       {@code TIME_SERIES_SET} 不接受 Function。官方对 GeoPoint 只写了
 *       "Initialized from an object property"，对后两者干脆没提 function</li>
 * </ol>
 * 注意官方那份列表自称 "can include options <b>such as</b> the following"（非穷举），
 * 且产品已新增官方文档未列出的定义方式（如 {@code Struct field}），因此本表是<b>保守下界</b>：
 * 它保证「选完类型一定还有得选」，代价是可能略窄于产品实际能力。
 *
 * <p><b>新增一个子类时，除了上面三步，还必须把它的 descriptor 登记进
 * {@link #TYPE_DEFINITIONS} 至少一处</b>——否则该定义方式不会出现在任何类型的下拉里，
 * 且编译器不会报错。{@code TestVariableTypeDefinitions} 对这一点有断言兜底。
 */
public abstract class VariableDefinitionConfig implements Describable<VariableDefinitionConfig> {

  /**
   * 变量类型 → 可产出的定义方式 descriptor 类。
   *
   * <p>值的类型是 {@code Class<? extends BasicDescriptor>} 而非 {@code Class<BasicDescriptor>}：
   * 泛型不变，{@code StaticConfig.DefaultDescriptor.class} 是
   * {@code Class<StaticConfig.DefaultDescriptor>}，塞不进 {@code List<Class<BasicDescriptor>>}。
   *
   * <p>用静态块而非 {@code Map.of(...)} 构造：{@code Map.of} 只有到 10 对 key-value 的
   * overload，而 {@link VariableType} 有 12 个常量。
   *
   * @see #supports(VariableType, Descriptor)
   * @see #filter(VariableType, List)
   */
  public static final Map<VariableType, List<Class<? extends BasicDescriptor>>> TYPE_DEFINITIONS;

  static {
    Map<VariableType, List<Class<? extends BasicDescriptor>>> defs = Maps.newHashMap();

    defs.put(VariableType.STRING, List.of(//
      StaticConfig.DefaultDescriptor.class //
      , ObjectPropertyConfig.DefaultDescriptor.class //
      , SQLQueryConfig.DefaultDescriptor.class //
      , VariableTransformationConfig.DefaultDescriptor.class //
      , FunctionConfig.DefaultDescriptor.class));

    defs.put(VariableType.BOOLEAN, List.of(//
      StaticConfig.DefaultDescriptor.class //
      , ObjectPropertyConfig.DefaultDescriptor.class //
      , FunctionConfig.DefaultDescriptor.class));

    defs.put(VariableType.DATE, List.of(//
      StaticConfig.DefaultDescriptor.class //
      , ObjectPropertyConfig.DefaultDescriptor.class //
      , SQLQueryConfig.DefaultDescriptor.class //
      , FunctionConfig.DefaultDescriptor.class));

    defs.put(VariableType.TIMESTAMP, List.of(//
      StaticConfig.DefaultDescriptor.class //
      , ObjectPropertyConfig.DefaultDescriptor.class //
      , SQLQueryConfig.DefaultDescriptor.class //
      , FunctionConfig.DefaultDescriptor.class));

    defs.put(VariableType.NUMERIC, List.of(//
      StaticConfig.DefaultDescriptor.class //
      , ObjectPropertyConfig.DefaultDescriptor.class //
      // 集合聚合唯一存活的类型：count/sum/average 只产出数值
      , ObjectSetAggregationConfig.DefaultDescriptor.class //
      , SQLQueryConfig.DefaultDescriptor.class //
      , FunctionConfig.DefaultDescriptor.class));

    defs.put(VariableType.GEOPOINT, List.of(//
      // 官方：GeoPoint only "Initialized from an object property"
      ObjectPropertyConfig.DefaultDescriptor.class));

    defs.put(VariableType.GEOSHAPE, List.of(//
      ObjectPropertyConfig.DefaultDescriptor.class //
      , FunctionConfig.DefaultDescriptor.class));

    defs.put(VariableType.ARRAY, List.of(//
      StaticConfig.DefaultDescriptor.class //
      , VariableTransformationConfig.DefaultDescriptor.class //
      , FunctionConfig.DefaultDescriptor.class));

    defs.put(VariableType.STRUCT, List.of(//
      StaticConfig.DefaultDescriptor.class //
      , FunctionConfig.DefaultDescriptor.class));

    defs.put(VariableType.OBJECT_SET, List.of(//
      // 官方明说 object set 不能由 SQL query 产出
      ObjectSetDefinitionConfig.DefaultDescriptor.class //
      , FunctionConfig.DefaultDescriptor.class));

    defs.put(VariableType.OBJECT_SET_FILTER, List.of(//
      // 值的唯一现实来源：图表刷选 / 过滤组件的运行时写入（见 ExternalInputConfig）
      ExternalInputConfig.DefaultDescriptor.class //
      , StaticConfig.DefaultDescriptor.class //
      , VariableTransformationConfig.DefaultDescriptor.class));

    defs.put(VariableType.TIME_SERIES_SET, List.of(//
      // 「单个对象的时间序列属性」，即对象属性
      ObjectPropertyConfig.DefaultDescriptor.class));

    TYPE_DEFINITIONS = Collections.unmodifiableMap(defs);
  }

  /**
   * 取某变量类型可用的全部定义方式 descriptor 类。
   *
   * @param type 变量类型；为 {@code null} 时返回空列表（调用方应改用
   *             {@link #supports(VariableType, Descriptor)}，它会放行）
   */
  public static List<Class<? extends BasicDescriptor>> definitionsOf(VariableType type) {
    if (type == null) {
      return Collections.emptyList();
    }
    return TYPE_DEFINITIONS.getOrDefault(type, Collections.emptyList());
  }

  /**
   * 判断某定义方式能否产出指定的变量类型。
   *
   * <p>用 {@link Class#isInstance(Object)} 而非泛型比较，是为了让调用方（
   * {@code WorkshopVariable} 的落盘校验）不必把
   * {@code Descriptor#getClass()} 强转成 {@code Class<? extends BasicDescriptor>}。
   *
   * @param type 变量类型。<b>{@code null} 一律放行</b>——无表单上下文时不能把候选清空，
   *             否则会出现「选了类型却没得选」的死角（参见 {@code MetadataOfValueType}
   *             中那句警告，以及 {@code ConstraintsOfValueType.getMetaStepQuietly()} 的
   *             空上下文 fallback）
   * @param defDesc 定义方式的 descriptor 实例；为 {@code null} 时返回 {@code false}
   */
  public static boolean supports(VariableType type, Descriptor<? extends VariableDefinitionConfig> defDesc) {
    if (type == null) {
      return true;
    }
    if (defDesc == null) {
      return false;
    }
    for (Class<? extends BasicDescriptor> supported : definitionsOf(type)) {
      if (supported.isInstance(defDesc)) {
        return true;
      }
    }
    return false;
  }

  /**
   * 按变量类型过滤候选定义方式，供表单侧 {@code subDescEnumFilter} 钩子直接调用。
   *
   * <p>钩子接线见 {@code WorkshopVariable.BasicDescriptor} 的构造器 —— 每个变量类型子类把
   * 自己的 {@link VariableType} 常量烙进 {@code definitionConfig} 字段的
   * {@code subDescEnumFilter}；作用点是 {@code PropertyType.applicableDescriptors(boolean)}
   * （在表单元数据构建期对本字段的全部候选 impl 应用一段 Groovy 函数）。
   *
   * <p>注意 Groovy 脚本那一侧请走 {@link #applicableDefinitions(VariableType, List)}：
   * 框架下发的是 {@code List<? extends Descriptor>}，本方法的 {@code D} 型参收不了。
   *
   * @param type       当前已选的变量类型；<b>{@code null} 时原样返回全部候选</b>，
   *                   与 {@link #supports(VariableType, Descriptor)} 的放行口径一致
   * @param candidates 全部候选 descriptor
   * @return 过滤后的候选；{@code type} 为 {@code null} 时即 {@code candidates} 本身
   */
  public static <D extends BasicDescriptor> List<D> filter(VariableType type, List<D> candidates) {
    if (type == null || candidates == null) {
      return candidates;
    }
    List<Class<? extends BasicDescriptor>> supported = definitionsOf(type);
    List<D> filtered = new ArrayList<>(candidates.size());
    for (D candidate : candidates) {
      if (supported.contains(candidate.getClass())) {
        filtered.add(candidate);
      }
    }
    return filtered;
  }

  /**
   * 按变量类型过滤候选定义方式，供<strong>变量类型子类</strong>注入的
   * {@code subDescEnumFilter} Groovy 脚本调用。
   *
   * <p>与 {@link #filter(VariableType, List)} 的<em>语义完全相同</em>，差别只在签名：
   * {@code filter} 的元素类型是 {@code D extends BasicDescriptor}，而 Groovy 脚本从框架拿到的
   * 是 {@code List<? extends Descriptor>}（{@code Descriptor} 上并无 {@code BasicDescriptor}
   * 约束），且 {@code filter} 的返回类型要保住元素的静态类型。
   * 两者<strong>不能</strong>写成同名重载 —— 擦除后都是 {@code (VariableType, List)}，会冲突。
   *
   * <p>接线方是 {@code WorkshopVariable.BasicDescriptor} 的构造器：每个变量类型子类把自己的
   * {@link VariableType} 常量烙进脚本，于是「类型 → 可用定义方式」的收敛从
   * <strong>跨步骤的运行时上下文</strong>变成<strong>单类内的静态事实</strong>。
   *
   * @param type       子类代表的变量类型；为 {@code null} 时原样返回全部候选
   * @param candidates 本字段的全部候选 descriptor（由框架下发）
   * @see #supports(VariableType, Descriptor)
   */
  public static List<? extends Descriptor> applicableDefinitions(
          VariableType type, List<? extends Descriptor> candidates) {
    if (type == null || candidates == null) {
      return candidates;
    }
    List<Class<? extends BasicDescriptor>> supported = definitionsOf(type);
    List<Descriptor> filtered = new ArrayList<>(candidates.size());
    for (Descriptor candidate : candidates) {
      if (supported.contains(candidate.getClass())) {
        filtered.add(candidate);
      }
    }
    return filtered;
  }

  /**
   * 该定义方式的值是否必须由后端计算。
   *
   * <p>前后端分工（设计文档 03-variable-system.md）：
   * <ul>
   *   <li><b>后端计算</b>（返回 {@code true}）：function / object-set-aggregation / sql-query。
   *       理由：SQL 与函数执行需要安全隔离（避免注入），大规模聚合在后端做可减轻前端内存压力。</li>
   *   <li><b>前端计算</b>（返回 {@code false}）：static / object-property /
   *       object-set-definition / variable-transformation。理由：交互类变量实时计算，
   *       减少网络往返。</li>
   * </ul>
   *
   * @return {@code true} 表示值由后端计算服务产出，前端应委托后端；{@code false} 表示可前端计算
   */
  public abstract boolean isBackendComputed();

  /**
   * 定义方式的 descriptor 基类。
   *
   * <p>可见性为 {@code public}（原为 {@code protected}）：{@link #TYPE_DEFINITIONS} 的
   * 值类型 {@code Class<? extends BasicDescriptor>} 需要被 {@code ...workshop.model} 包下的
   * {@code WorkshopVariable} 与表单侧的 Groovy 过滤脚本引用，protected 跨包不可见。
   * 形态对齐 TIS 中同类先例 {@code ValueConstraint.BaseDesc}（同样是 public static abstract）。
   */
  public abstract static class BasicDescriptor extends Descriptor<VariableDefinitionConfig> {
    // 公共描述符逻辑
  }
}
