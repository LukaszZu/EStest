package zz.test;

import com.google.common.collect.ImmutableList;
import org.modelmapper.*;
import org.modelmapper.spi.PropertyMapping;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class MapperTest {
    public static void main(String[] args) {
        ModelMapper modelMapper = new ModelMapper();

        ImmutableList<SrcModel> src = ImmutableList.of(new SrcModel("id", "name", "src"));

//        Type dstType = new TypeToken<List<DstModel>>() {
//        }.getType();
//
//        List<DstModel> model = modelMapper
//                .map(src, dstType);

//        TypeMap<SrcModel, DstModel> typeMap = modelMapper.createTypeMap(SrcModel.class, DstModel.class);

        TypeMap<SrcModel, DstModel> typeMap = modelMapper.createTypeMap(SrcModel.class, DstModel.class);

        Condition isNotNull = ctx -> ctx.getSource() != null;

        typeMap.addMappings(mapper -> mapper.when(isNotNull)
                .map(SrcModel::getSrc, DstModel::setDst));

        Map<String, String> propertyMap = typeMap.getMappings().stream()
                .map(PropertyMapping.class::cast)
                .collect(
                        toMap(m -> m.getLastSourceProperty().getName(),
                                m -> m.getLastDestinationProperty().getName())
                );

        System.out.println(propertyMap);

        typeMap.getMappings().forEach(System.out::println);

        src.stream()
                .map(typeMap::map)
                .collect(toList())
                .forEach(System.out::println);

    }
}
