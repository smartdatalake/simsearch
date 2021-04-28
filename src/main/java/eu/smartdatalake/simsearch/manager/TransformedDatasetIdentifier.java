package eu.smartdatalake.simsearch.manager;

import eu.smartdatalake.simsearch.manager.DataType.Type;
import eu.smartdatalake.simsearch.manager.ingested.lookup.Word2VectorTransformer;

/**
 * Provides identification of a transformed dataset, i.e., sets of keywords converted to a vector of double values according to a given dictionary of words.  
 */
public class TransformedDatasetIdentifier extends DatasetIdentifier {

	private Word2VectorTransformer transformer;
	
	// Associate this transformed dataset with its original one
	private DatasetIdentifier original = null; // There must always be an original dataset that led to this transformed one

	/**
	 * Constructor
	 * @param source  The data source (directory, JDBC connection, or REST API) that provides access to the attribute data.
	 * @param dataset  The name of the dataset (file, table) that contains the attribute data.
	 * @param colValueName  The column name containing the attribute data to be used.
	 * @param nameOperation  The name of the operation supported on this attribute.
	 * @param transformed  Boolean indicating whether this data need be transformed.
	 */
	public TransformedDatasetIdentifier(DataSource source, String dataset, String colValueName, String nameOperation, boolean transformed) {
		
		super(source, dataset, colValueName, nameOperation, transformed);
	}


	/**
	 * Provides the word2vec transformer using as vocabulary the dataset of this identifier.
	 * @return  The transformer to be used for word2vec computations.
	 */
	public Word2VectorTransformer getTransformer() {
		return transformer;
	}

	/**
	 * Specifies the word2vec transformer using as vocabulary the dataset of this identifier.
	 * @param transformer  The transformer to be used for word2vec computations.
	 */
	public void setTransformer(Word2VectorTransformer transformer) {
		this.transformer = transformer;
	}


	@Override
	public boolean isTransformed() {
		return true;
	}

	@Override
	public boolean needsTransform() {
		return (transformer != null);
	}

	/**
	 * Provides the identifier of the original dataset that got transformed.
	 * @return  The dataset identifier of the original attribute data.
	 */
	public DatasetIdentifier getOriginal() {
		return original;
	}


	/**
	 * Specifies the identifier of the original dataset that got transformed.
	 * @param original  The dataset identifier of the original attribute data.
	 */
	public void setOriginal(DatasetIdentifier original) {
		// FIXME: Assuming that the original attribute always represents sets of keywords  
		setDatatype(Type.KEYWORD_SET);
		this.original = original;
	}
	
}
