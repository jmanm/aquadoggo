use std::collections::HashMap;

use p2panda_rs::{schema::FieldType, test_utils::fixtures::random_key_pair};

use crate::{aquadoggo_rpc::{connect_client::ConnectClient, field::Value, DocumentRequest}, test_utils::{add_document, add_schema, test_runner, TestNode}};

#[test]
fn scalar_fields() {
    test_runner(|mut node: TestNode| async move {
        let key_pair = random_key_pair();

        // Add schema to node
        let schema = add_schema(
            &mut node,
            "schema_name",
            vec![
                ("bool", FieldType::Boolean),
                ("float", FieldType::Float),
                ("int", FieldType::Integer),
                ("text", FieldType::String),
                ("bytes", FieldType::Bytes),
            ],
            &key_pair,
        )
        .await;

        // Publish document on node
        let doc_fields = vec![
            ("bool", true.into()),
            ("float", (1.0).into()),
            ("int", 1.into()),
            ("text", "yes".into()),
            ("bytes", vec![0, 1, 2, 3][..].into()),
        ];
        let view_id = add_document(&mut node, schema.id(), doc_fields.clone(), &key_pair).await;

        let mut client = ConnectClient::connect("http://localhost:2021").await.unwrap();
        let mut request = DocumentRequest::default();
        request.document_view_id = Some(view_id.to_string());

        let response = client.get_document(request)
            .await
            .unwrap()
            .into_inner();

        assert_ne!(response.document, None);
        let doc = response.document.unwrap();
        assert!(!doc.fields.is_empty());

        let mut field_map = HashMap::new();
        for field in doc.fields {
            field_map.insert(field.name.clone(), field);
        }

        assert_eq!(field_map.len(), doc_fields.len());
        assert!(matches!(field_map.get("bool").unwrap().value, Some(Value::BoolVal(true))));
    });
}
