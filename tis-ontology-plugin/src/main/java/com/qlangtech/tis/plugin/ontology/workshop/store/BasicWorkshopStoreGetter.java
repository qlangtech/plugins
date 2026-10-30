package com.qlangtech.tis.plugin.ontology.workshop.store;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.plugin.ontology.impl.storegetter.BaiscAssistStoreGetter;
import com.qlangtech.tis.plugin.ontology.workshop.WorkshopModule;

import java.io.File;
import java.util.Optional;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/21
 */
public abstract class BasicWorkshopStoreGetter<T extends Describable<?>> extends BaiscAssistStoreGetter<T> {


    @Override
    public File getAssistRootDir(String ontologyName, Optional<String> submodule) {
        return WorkshopModule.getWorkshopDir(ontologyName
                , submodule.orElseThrow(() -> new IllegalStateException("must contain submodule name in:" + this.getClass().getName())));
    }

    @Override
    public File getAssistRootDir(String ontologyName) {
        // return super.getAssistRootDir(ontologyName);
        throw new UnsupportedOperationException("ontologyName:" + ontologyName);
    }

//        /**
//         * 与父类实现逐行一致，唯一的差别是 Key 的 pluginClass 取自 {@link #getPluginType()}
//         * 而不是硬编码的 {@code ONTOLOGY.extensionPoint}。
//         * <p>
//         * 之所以整段就地覆盖而不是调用 super：Key 是父类在方法体内匿名构造的，
//         * 外部没有替换其 pluginClass 的机会，只能自己再构造一遍。
//         * 父类若改动落盘约定，这里需要同步跟进。
//         */
//        @SuppressWarnings("all")
//        @Override
//        public IPluginStore<T> getPluginStore(OntologyPluginMeta pluginMeta, Optional<String> submodule) {
//            if (pluginMeta.isUpdate() || pluginMeta.shallPersistence()) {
//                String ontologyName = pluginMeta.getDomain();
//                final String pluginIdVal = pluginMeta.getPluginIdVal();
//                if (StringUtils.isEmpty(ontologyName)) {
//                    throw new IllegalArgumentException("param ontologyName can not be empty");
//                }
//                if (StringUtils.isEmpty(pluginIdVal)) {
//                    throw new IllegalArgumentException("param pluginIdVal can not be empty");
//                }
//                KeyedPluginStore.Key key = new KeyedPluginStore.Key(Ontology.ONTOLOGY.getIdentity(), ontologyName
//                        , getPluginType()) {
//                    @Override
//                    public File getStoreFile() {
//                        File workshopDir = getAssistRootDir(ontologyName, submodule);
//                        return new File(workshopDir, Descriptor.getPluginFileName(getFileName()));
//                    }
//
//                    public String getSerializeFileRelativePath() {
//                        return this.getSubDirPath() + File.separator + getFileName();
//                    }
//
//                    @Override
//                    protected String getFileName() {
//                        return pluginIdVal;
//                    }
//
//                    @Override
//                    public int hashCode() {
//                        return getStoreFile().hashCode();
//                    }
//                };
//                return TIS.getPluginStore(key).unsaveCast();
//            }
//            return (IPluginStore<T>) IPluginStore.noSaveStore(pluginMeta.getDelegate());
//        }
}
